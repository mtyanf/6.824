package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// example to show how to declare the arguments
// and reply for an RPC.
const (
	WAIT   = 0
	MAP    = 1
	REDUCE = 2
	FINISH = 3
)

// Add your RPC definitions here.

type GetTaskArgs struct {
	// self information for separate machine
}

type GetTaskReply struct {
	Files   []string
	NMap    int
	NReduce int
	Task    int
	TaskID  int
}

type FinishTaskArgs struct {
	TaskID int
	Task   int
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
