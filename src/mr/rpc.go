package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Type         string
	TaskNum      int32
	FileName     string // used fo map task
	NReduce      int    // used fo map task for partitioning
	TotalFileCnt int    // used for reduce task, to get the X in mr-X-Y
	NeedWait     bool   // if this is true, client is asked to wait and retry
}

type ReportSuccessArgs struct {
	Type    string
	TaskNum int32
}

type ReportSuccessReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
