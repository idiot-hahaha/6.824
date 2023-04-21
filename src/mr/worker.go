package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

var threadID WorkerID

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

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

	threadID = WorkerID(os.Getpid())
	link()
	go ping()

	for {
		var TaskInfo GetTaskReply
		var args = GetTaskArgs{
			ID: threadID,
		}
		if ok := call("Coordinator.GetTask", &args, &TaskInfo); !ok {
			break
		}
		if TaskInfo.Task == MapTask {
			//fmt.Println("doMap:", TaskInfo.TaskID)
			doMap(mapf, TaskInfo.TaskID, TaskInfo.Inames[0], TaskInfo.NReduce)
			finishMap(TaskInfo.TaskID)
		} else if TaskInfo.Task == ReduceTask {
			//fmt.Println("doReduce:", TaskInfo.TaskID)
			doReduce(reducef, TaskInfo.TaskID, TaskInfo.NMap)
			finishReduce(TaskInfo.TaskID)
		} else if TaskInfo.Task == Complete {
			break
		} else {
			//fmt.Println("sleep")
			time.Sleep(time.Second)
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, taskID TaskID, filename string, nReduce int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucketsIndex := ihash(kv.Key) % 10
		buckets[bucketsIndex] = append(buckets[bucketsIndex], kv)
	}
	for idx, bucket := range buckets {
		ofile, _ := os.CreateTemp("", "out")
		enc := json.NewEncoder(ofile)
		for _, kv := range bucket {
			enc.Encode(&kv)
		}
		ofile.Close()
		oname := mapOutFilePath(taskID, TaskID(idx))
		os.Rename(ofile.Name(), oname)
	}
	return
}

func mapOutFilePath(taskID TaskID, reduceIndex TaskID) string {
	path := fmt.Sprintf("mr-%d-%d", taskID, reduceIndex)
	return path
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(reducef func(string, []string) string, rTaskID TaskID, nMap int) {

	intermediate := []KeyValue{}
	for taskID := 0; taskID < nMap; taskID++ {
		iname := mapOutFilePath(TaskID(taskID), rTaskID)
		ifile, _ := os.Open(iname)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ifile.Close()
	}
	ofile, _ := os.CreateTemp("", "tempOut")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//

	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	oname := fmt.Sprintf("mr-out-%d", rTaskID)
	os.Rename(ofile.Name(), oname)
}

func finishMap(id TaskID) {
	args := FinishMapArgs{
		TaskID: id,
	}
	reply := FinishMapReply{}
	call("Coordinator.FinishMap", &args, &reply)
}

func finishReduce(taskID TaskID) {
	args := FinishReduceArgs{
		TaskID: taskID,
	}
	reply := FinishReduceReply{}
	call("Coordinator.FinishReduce", &args, &reply)
}

func link() {
	args := &LinkArgs{
		ID: threadID,
	}
	reply := &LinkReply{}
	call("Coordinator.Link", args, reply)
}

func ping() {
	args := &PingArgs{
		ID: threadID,
	}
	reply := &PingReply{}
	for {
		time.Sleep(time.Millisecond * 500)
		call("Coordinator.Ping", args, reply)
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
