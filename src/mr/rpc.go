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

type TaskType int

type TaskStatus int

const (
	UnknownTaskType TaskType = iota
	MapTask
	ReduceTask
	AllTaskDone
)

const (
	UnknownTaskStatus TaskStatus = iota
	Waiting
	Running
	Done
)

type Task struct {
	ID        int
	NReduce   int
	Type      TaskType
	Status    TaskStatus
	StartTime int64
	FileName  string
}

type GetTaskArgs struct{}

type GetTaskReply struct {
	Task *Task
}

type TaskDoneOrArgs struct {
	Task *Task
}

type TaskDoneOrReply struct{}
