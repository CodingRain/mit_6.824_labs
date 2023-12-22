package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const RETRY_INTERVAL_SECONDS = 1
const RETRY_TIMES = 10

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// infinite loop
	for {
		task := GetTask()
		if task.Type == "map" {
			HandleMapTask(task, mapf)
		} else if task.Type == "reduce" {
			HandleReduceTask(task, reducef)
		} else if task.Type == "all_finished" {
			os.Exit(0)
			// ?
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func GetTask() GetTaskReply {
	for i := 0; i < RETRY_TIMES; i++ {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			if reply.NeedWait {
				fmt.Printf("server ask me to wait! retry time %v...\n", i)
				time.Sleep(time.Second * RETRY_INTERVAL_SECONDS)
				continue
			}
			fmt.Printf("get task success reply=%+v\n", reply)
			return reply
		} else {
			fmt.Printf("get task failed")
			os.Exit(1)
		}
	}
	// 10 times and failed, simply exit
	fmt.Println("retry time reach maximu, exit...")
	os.Exit(2)
	// will not be executed
	return GetTaskReply{}
}

func ReportSuccess(taskType string, taskNum int32) {
	args := ReportSuccessArgs{}
	args.Type = taskType
	args.TaskNum = taskNum
	reply := ReportSuccessReply{}
	ok := call("Coordinator.ReportSuccess", &args, &reply)
	if ok {
		fmt.Printf("reply=%+v\n", reply)
	} else {
		fmt.Printf("call reportSuccess failed!\n")
	}
}

func HandleMapTask(task GetTaskReply, mapf func(string, string) []KeyValue) {
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return
	}
	file.Close()
	kva := mapf(filename, string(content))
	var outputFiles []*os.File
	var outputEncoders []*json.Encoder
	for i := 0; i < task.NReduce; i++ {
		name := fmt.Sprintf("mr-%d-%d", task.TaskNum, i)
		outputFile, err := os.CreateTemp("", name)
		if err != nil {
			_ = fmt.Errorf("fatal error: create file failed, err=%v", err)
			return
		}
		defer outputFile.Close()
		outputFiles = append(outputFiles, outputFile)
		enc := json.NewEncoder(outputFile)
		outputEncoders = append(outputEncoders, enc)
		err = os.Rename(outputFile.Name(), name)
		if err != nil {
			_ = fmt.Errorf("fatal error: rename file failed, err=%v", err)
			return
		}
	}
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % task.NReduce
		outputEncoders[reduceTaskNum].Encode(&kv)
	}
	ReportSuccess(task.Type, task.TaskNum)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func HandleReduceTask(task GetTaskReply, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for i := 0; i < task.TotalFileCnt; i++ {
		name := fmt.Sprintf("mr-%d-%d", i, task.TaskNum)
		file, err := os.Open(name)
		if err != nil {
			_ = fmt.Errorf("fatal error: open intermediate file failed, err=%v", err)
			return
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	fmt.Println(len(kva))

	sort.Sort(ByKey(kva))

	ofileName := fmt.Sprintf("mr-out-%d", task.TaskNum)
	ofile, err := os.CreateTemp("", ofileName)
	if err != nil {
		_ = fmt.Errorf("fatal error: create output file failed, err=%v", err)
		return
	}
	defer ofile.Close()
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	err = os.Rename(ofile.Name(), ofileName)
	if err != nil {
		_ = fmt.Errorf("fatal error: rename file failed, err=%v", err)
		return
	}

	ReportSuccess(task.Type, task.TaskNum)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
