package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
const (
	Sleep = iota
	MapTask
	ReduceTask
	Complete
)

type GetTaskArgs struct {
	ID WorkerID
}

type GetTaskReply struct {
	Task    int
	TaskID  TaskID
	NReduce int
	NMap    int
	Inames  []string
}

type FinishMapArgs struct {
	TaskID TaskID
}

type FinishMapReply struct {
}

type FinishReduceArgs struct {
	TaskID TaskID
}

type FinishReduceReply struct {
}

type LinkArgs struct {
	ID WorkerID
}

type LinkReply struct {
}
type PingArgs struct {
	ID WorkerID
}

type PingReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
