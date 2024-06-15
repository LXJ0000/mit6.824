package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const timeout = time.Second * 10

type Coordinator struct {
	// Your definitions here.
	nMap    int
	nReduce int

	finishedMap    int
	finishedReduce int

	mapTaskLog    []int
	reduceTaskLog []int

	files []string

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) FinishedMap(args *WorkerArgs, reply *WorkerReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finishedMap++
	c.mapTaskLog[args.MapID] = 2
}

func (c *Coordinator) FinishedReduce(args *WorkerArgs, reply *WorkerReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finishedReduce++
	c.reduceTaskLog[args.ReduceID] = 2
}

func (c *Coordinator) AllocateTask(args *WorkerArgs, reply *WorkerReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finishedMap < c.nMap {
		allocate := -1
		for i := 0; i < c.nMap; i++ {
			if c.mapTaskLog[i] == 0 { // 未分配、未开始
				allocate = i
				break
			}
		}
		if allocate == -1 {
			reply.Type = WaitingTask // 等待任务分配
			return
		}
		reply.Type = MapTask
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap // TODO clear
		reply.FileName = c.files[allocate]
		reply.MapID = allocate
		c.mapTaskLog[allocate] = 1 // waiting
		go func() {
			time.Sleep(timeout)
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.mapTaskLog[allocate] == 1 { // More than 10 seconds (timeout) and still not completed
				c.mapTaskLog[allocate] = 0
			}
		}()
	} else if c.finishedMap == c.nMap && c.finishedReduce < c.nReduce {
		allocate := -1
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskLog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			reply.Type = WaitingTask
			return
		}
		reply.Type = ReduceTask
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
		reply.ReduceID = allocate
		c.reduceTaskLog[allocate] = 1 // waiting
		go func() {
			time.Sleep(timeout)
			if c.reduceTaskLog[allocate] == 1 {
				c.reduceTaskLog[allocate] = 0
			}
		}()
	} else {
		reply.Type = JobFinished
	}

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
	ret = c.nReduce == c.finishedReduce
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.nMap = len(files)
	c.files = files
	c.mapTaskLog = make([]int, c.nMap)
	c.reduceTaskLog = make([]int, c.nReduce)

	c.server()
	return &c
}
