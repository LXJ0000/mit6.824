package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
	for {
		task := callGetTask()
		if task == nil {
			fmt.Println("task is nil, continue")
			time.Sleep(2 * time.Second)
			continue
		}
		fmt.Printf("worker: receive coordinators get task: %v\n", task)
		switch task.Type {
		case MapTask:
			doMap(mapf, task)
		case ReduceTask:
			doReduce(reducef, task)
		case AllTaskDone:
			return
		default:
			fmt.Printf("worker: unknow task type: %v\n", task.Type)
			time.Sleep(2 * time.Second)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMap(mapf func(string, string) []KeyValue, task *Task) {}

func doReduce(reducef func(string, []string) string, task *Task) {}

func callGetTask() *Task {
	args := GetTaskArgs{}

	reply := GetTaskReply{}

	if ok := call("Coordinator.GetTask", &args, &reply); !ok {
		fmt.Println("call failed!")
	}
	return reply.Task
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
