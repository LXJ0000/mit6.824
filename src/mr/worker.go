package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		args := WorkerArgs{}
		reply := WorkerReply{}
		ok := call("Coordinator.AllocateTask", &args, &reply)
		if !ok || reply.Type == JobFinished {
			fmt.Println("Coordinator.AllocateTask failed Or Job finished.")
			return
		}
		switch reply.Type {
		case MapTask:
			doMap(mapf, &reply)
		case ReduceTask:
			doReduce(reducef, &reply)
		case WaitingTask:
			fmt.Println("Coordinator waiting task.")
		default:
			fmt.Println("Coordinator doesn't know how to do anything with unknown type.")
		}
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMap(mapf func(string, string) []KeyValue, reply *WorkerReply) {
	// open and read file
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("Open failed with '%s'\n", err)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("ReadAll failed with '%s'\n", err)
	}
	// call mapf
	intermediate := mapf(reply.FileName, string(content))
	// hash bucketing
	buckets := make([][]KeyValue, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		buckets[i] = []KeyValue{}
	}
	for _, item := range intermediate {
		buckets[ihash(item.Key)%reply.NReduce] = append(buckets[ihash(item.Key)%reply.NReduce], item)
	}
	// write to tmp file
	for i := range buckets {
		oName := strings.Join([]string{"mr", strconv.Itoa(reply.MapID), strconv.Itoa(i)}, "-") // mr-map-reduce
		oFile, _ := os.CreateTemp("", oName+"*")
		encoder := json.NewEncoder(oFile)
		for _, item := range buckets[i] {
			if err := encoder.Encode(item); err != nil {
				log.Fatalf("encode failed with '%s'\n", err)
			}
		}
		_ = os.Rename(oFile.Name(), oName)
		_ = oFile.Close()
	}
	// call coordinator to send the finish message
	finishedArgs := WorkerArgs{MapID: reply.MapID, ReduceID: -1}
	finishedReply := WorkerReply{}
	call("Coordinator.FinishedMap", &finishedArgs, &finishedReply)
}

func doReduce(reducef func(string, []string) string, reply *WorkerReply) {
	var intermediate []KeyValue
	for i := 0; i < reply.NMap; i++ {
		iName := strings.Join([]string{"mr", strconv.Itoa(i), strconv.Itoa(reply.ReduceID)}, "-")
		file, err := os.Open(iName)
		if err != nil {
			log.Fatalf("Open failed with '%s'\n", err)
		}
		decoder := json.NewDecoder(file)
		for {
			var item KeyValue
			if err := decoder.Decode(&item); err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("Decode failed with '%s'\n", err)
			}
			intermediate = append(intermediate, item)
		}
		file.Close()
	}
	// sort by key
	sort.Sort(ByKey(intermediate))

	// output file
	oName := strings.Join([]string{"mr", "out", strconv.Itoa(reply.ReduceID)}, "-")
	oFile, _ := os.CreateTemp("", oName+"*")

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	_ = os.Rename(oFile.Name(), oName)
	_ = oFile.Close()

	// remove temp file
	for i := 0; i < reply.NMap; i++ {
		iName := strings.Join([]string{"mr", strconv.Itoa(i), strconv.Itoa(reply.ReduceID)}, "-")
		err := os.Remove(iName)
		if err != nil {
			log.Fatalf("Remove failed with '%s'\n", err)
		}
	}
	// call coordinator to send the finish message
	finishedArgs := WorkerArgs{MapID: -1, ReduceID: reply.ReduceID}
	finishedReply := WorkerReply{}
	call("Coordinator.FinishedReduce", &finishedArgs, &finishedReply)
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
