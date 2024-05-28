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

type worker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := &GetTaskArgs{}
		reply := &GetTaskReply{}

		call("Coordinator.GetTask", args, reply)

		switch reply.Task {
		case WAIT:
			time.Sleep(3 * time.Second)
		case MAP:
			StartMap(reply.Files, reply.TaskID, reply.NReduce, mapf)
		case REDUCE:
			StartReduce(reply.Files, reply.TaskID, reply.NMap, reply.NReduce, reducef)
		case FINISH:
			break
		}
	}

}

func StartMap(files []string, taskID, nReduce int,
	mapf func(string, string) []KeyValue) {
	// mr-X-Y
	intermediate := make([][]KeyValue, nReduce)
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		_ = file.Close()
		kva := mapf(filename, string(content))
		for i := range kva {
			r := ihash(kva[i].Key) % nReduce
			intermediate[r] = append(intermediate[r], kva[i])
		}
	}

	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", taskID, i)
		file, err := os.CreateTemp("", filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		enc := json.NewEncoder(file)
		for _, kv := range intermediate[i] {
			_ = enc.Encode(kv)
		}

		_ = os.Rename(file.Name(), filename)
	}

	args := &FinishTaskArgs{}
	reply := &FinishTaskReply{}
	args.TaskID = taskID
	args.Task = MAP
	call("Coordinator.FinishTask", args, reply)
}

func StartReduce(files []string, taskID, nMap, nReduce int,
	reducef func(string, []string) string) {

	kva := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, taskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Slice(kva, func(x, y int) bool {
		return kva[x].Key < kva[y].Key
	})

	i := 0
	filename := fmt.Sprintf("mr-out-%d", taskID)
	file, err := os.CreateTemp("", filename)
	if err != nil {
		log.Fatalf("cannot create %v", filename)
	}
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		_, _ = fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		i = j
	}

	_ = os.Rename(file.Name(), filename)

	args := &FinishTaskArgs{}
	reply := &FinishTaskReply{}
	args.TaskID = taskID
	args.Task = REDUCE
	call("Coordinator.FinishTask", args, reply)
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
