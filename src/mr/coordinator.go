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

var clock time.Timer

const (
	TimeOut = time.Second * 10
)

type phase int

const (
	duringMap phase = iota
	duringReduce
	finish
)

type Coordinator struct {
	// Your definitions here.
	tasks map[TaskType][]*Task

	nMap    int
	nReduce int

	finishedMap    int
	finishedReduce int
	phase          phase

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.phase {
	case duringMap:
		reply.Task = findTask(c.tasks[MapTask])
	case duringReduce:
		reply.Task = findTask(c.tasks[ReduceTask])
	case finish:
		reply.Task = &Task{
			Type: AllTaskDone,
		}
	}
	return nil
}

func findTask(tasks []*Task) *Task {
	for _, task := range tasks {
		if task.Status == Waiting || (task.Status == Running && task.StartTime+TimeOut.Microseconds() > time.Now().UnixMicro()) {
			task.StartTime = time.Now().UnixMicro()
			task.Status = Running
			return task
		}
	}
	return nil
}

func (c *Coordinator) TaskDoneOr(args *TaskDoneOrArgs, reply *TaskDoneOrReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	taskType, id := args.Task.Type, args.Task.ID
	if taskType == MapTask && c.tasks[MapTask][id].Status != Done {
		c.tasks[MapTask][id] = args.Task
		c.finishedMap++
		if c.finishedMap == c.nReduce {
			c.phase = duringReduce
			fmt.Println("coordinator: all map tasks finished")
		}
	}
	if taskType == ReduceTask && c.tasks[ReduceTask][id].Status != Done {
		c.tasks[ReduceTask][id] = args.Task
		c.finishedReduce++
		if c.finishedReduce == c.nReduce {
			c.phase = finish
			fmt.Println("coordinator: all reduce tasks finished")
		}
		clock = *time.NewTimer(time.Second * 10)
		go func() {
			for {
				select {
				case <-clock.C:
					os.Exit(0)
				}
			}
		}()
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
