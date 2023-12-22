package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TASK_STATUS_UNSTARTED = 0 // default status
	TASK_STATUS_STARTED   = 1
	TASK_STATUS_TIMEOUT   = 2
	TASK_STATUS_FINISHED  = 3
)

const WORKER_TIMEOUT_SECONDS = 10

type Coordinator struct {
	// Your definitions here.
	files             []string
	nReduce           int
	finishedMapCnt    int
	finishedReduceCnt int
	mapTaskStatus     []int
	reduceTaskStatus  []int
	lock              sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 2
	return nil
}

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.files) != c.finishedMapCnt {
		// assign a map task for worker
		reply.Type = "map"
		reply.NReduce = c.nReduce
		for i := 0; i < len(c.mapTaskStatus); i++ {
			if c.mapTaskStatus[i] == TASK_STATUS_UNSTARTED || c.mapTaskStatus[i] == TASK_STATUS_TIMEOUT {
				reply.FileName = c.files[i]
				reply.TaskNum = int32(i)
				c.mapTaskStatus[i] = TASK_STATUS_STARTED
				go func(taskNum int) {
					// fmt.Printf("--------timing thread------- : %v is going to count\n", taskNum)
					time.Sleep(time.Second * WORKER_TIMEOUT_SECONDS)
					if c.mapTaskStatus[i] == TASK_STATUS_STARTED {
						c.mapTaskStatus[i] = TASK_STATUS_TIMEOUT
						// fmt.Printf("--------timing thread------- : %v timeout\n", taskNum)
					} else {
						// fmt.Printf("--------timing thread------- : %v finished, status=%v\n", taskNum, c.mapTaskStatus[i])
					}
				}(i)
				return nil
			}
		}
		// there's not task to allocate, maybe some map tasks are still executing.
		reply.Type = "map_unfinished"
		reply.NeedWait = true
		fmt.Printf("map task unfinshed, please wait, cur status=%v\n", c.mapTaskStatus)
		return nil
	} else if c.nReduce != c.finishedReduceCnt {
		// allowcate a reduce task for worker, but must first check all the map has finished
		reply.Type = "reduce"
		reply.TotalFileCnt = len(c.files)
		for i := 0; i < len(c.reduceTaskStatus); i++ {
			if c.reduceTaskStatus[i] == TASK_STATUS_UNSTARTED || c.reduceTaskStatus[i] == TASK_STATUS_TIMEOUT {
				reply.TaskNum = int32(i)
				c.reduceTaskStatus[i] = TASK_STATUS_STARTED
				go func(taskNum int) {
					time.Sleep(time.Second * WORKER_TIMEOUT_SECONDS)
					if c.reduceTaskStatus[i] == TASK_STATUS_STARTED {
						c.reduceTaskStatus[i] = TASK_STATUS_TIMEOUT
					}
				}(i)
				return nil
			}
		}
		// there's no task to allocate, maybe some reduce tasks are still executing
		reply.Type = "reduce_unfinished"
		reply.NeedWait = true
		fmt.Printf("reduce task unfinshed, please wait, cur status=%v\n", c.reduceTaskStatus)
		return nil
	} else {
		// client should exist on this
		fmt.Println("haha")
		reply.Type = "all_finished"
	}
	return nil
}

func (c *Coordinator) ReportSuccess(args *ReportSuccessArgs, reply *ReportSuccessReply) error {
	fmt.Printf("report success! args=%v\n", args)
	if args.Type == "map" {
		c.mapTaskStatus[args.TaskNum] = TASK_STATUS_FINISHED
		c.finishedMapCnt += 1
	} else if args.Type == "reduce" {
		c.reduceTaskStatus[args.TaskNum] = TASK_STATUS_FINISHED
		c.finishedReduceCnt += 1
	} else {
		_ = fmt.Errorf("unknown report success type!")
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.finishedMapCnt == len(c.files) && c.finishedReduceCnt == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		nReduce:           nReduce,
		finishedMapCnt:    0,
		finishedReduceCnt: 0,
		mapTaskStatus:     make([]int, len(files)),
		reduceTaskStatus:  make([]int, nReduce),
		lock:              sync.Mutex{},
	}

	// are these lines necessary?
	for i := 0; i < len(c.mapTaskStatus); i++ {
		c.mapTaskStatus[i] = TASK_STATUS_UNSTARTED
	}
	for i := 0; i < len(c.mapTaskStatus); i++ {
		c.reduceTaskStatus[i] = TASK_STATUS_UNSTARTED
	}

	// Your code here.

	c.server()
	return &c
}
