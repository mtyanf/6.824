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

const (
	NOTDO = 0
	DONE  = 1
	DOING = 2

	WAITTIME = 10
)

type Coordinator struct {
	// Your definitions here.
	tasks        [][]string
	nMap         int
	nReduce      int
	mapTasks     []int8
	reduceTasks  []int8
	mu           sync.Mutex
	finishMap    []chan struct{}
	finishReduce []chan struct{}
	done         bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if task, ok := c.findTask(c.mapTasks); !ok {
		if task == -1 { // let worker wait
			reply.Task = WAIT
			return nil
		}
		reply.Task = MAP
		reply.Files = c.tasks[task]
		reply.NReduce = c.nReduce
		reply.TaskID = task

		go c.startMap(task)
	} else if task, ok = c.findTask(c.reduceTasks); !ok {
		if task == -1 {
			reply.Task = WAIT
			return nil
		}
		// when using on separate machine, represent different map work machine
		// reply.Files = ?
		reply.Task = REDUCE
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.TaskID = task

		go c.startReduce(task)
	} else {
		reply.Task = FINISH
		c.done = true
	}
	return nil
}

func (c *Coordinator) findTask(tasks []int8) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	allDone := true
	for i := range tasks {
		if tasks[i] == NOTDO {
			tasks[i] = DOING
			return i, false
		} else if tasks[i] == DOING {
			allDone = false
		}
	}
	return -1, allDone
}

func (c *Coordinator) startMap(task int) {
	quit := time.After(WAITTIME * time.Second)
	select {
	case <-quit:
		c.mu.Lock()

		c.mapTasks[task] = NOTDO
	case <-c.finishMap[task]:
		c.mu.Lock()

		c.mapTasks[task] = DONE
	}
	c.mu.Unlock()
}

func (c *Coordinator) startReduce(task int) {
	quit := time.After(WAITTIME * time.Second)
	select {
	case <-quit:
		c.mu.Lock()

		c.reduceTasks[task] = NOTDO
	case <-c.finishReduce[task]:
		c.mu.Lock()

		c.reduceTasks[task] = DONE
	}
	c.mu.Unlock()
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	taskID := args.TaskID
	task := args.Task
	c.mu.Lock()
	defer c.mu.Unlock()

	if task == MAP {
		if c.mapTasks[taskID] != DOING {
			return nil
		}

		c.finishMap[taskID] <- struct{}{}
	} else if task == REDUCE {
		if c.reduceTasks[taskID] != DOING {
			return nil
		}
		c.finishReduce[taskID] <- struct{}{}
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
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	ret = c.done

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce
	for i := range files {
		c.tasks = append(c.tasks, []string{files[i]})
	}
	c.mapTasks = make([]int8, c.nMap)
	c.reduceTasks = make([]int8, nReduce)

	for i := 0; i < c.nMap; i++ {
		c.finishMap = append(c.finishMap, make(chan struct{}))
	}
	for i := 0; i < c.nReduce; i++ {
		c.finishReduce = append(c.finishReduce, make(chan struct{}))
	}

	c.server()
	return &c
}
