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
	inMap = iota
	inReduce
	complete
)

type Coordinator struct {
	// Your definitions here.
	nReduce              int
	state                chan int
	mapTasks             chan int
	reduceTasks          chan int
	mTaskUnassignedCount chan int
	rTaskUnassignedCount chan int
	mTaskCount           chan int
	rTaskCount           chan int
	mapCompleteMutex     sync.Mutex
	reduceCompleteMutex  sync.Mutex
	files                []string
	mapComplete          []bool
	reduceComplete       []bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	state := <-c.state
	if state == inMap {
		count := <-c.mTaskUnassignedCount
		c.state <- state
		if count == 0 {
			c.mTaskUnassignedCount <- 0
			reply.Task = Sleep
			return nil
		}
		taskID := <-c.mapTasks
		c.mTaskUnassignedCount <- count - 1
		reply.Task = MapTask
		reply.TaskID = taskID
		reply.NMap = len(c.files)
		reply.NReduce = c.nReduce
		reply.Inames = []string{c.files[taskID]}
		go func() {
			time.Sleep(time.Second * 10)
			c.mapCompleteMutex.Lock()
			if !c.mapComplete[taskID] {
				c.mapCompleteMutex.Unlock()
				c.mapTasks <- taskID
				mCount := <-c.mTaskUnassignedCount
				c.mTaskUnassignedCount <- mCount + 1
			} else {
				c.mapCompleteMutex.Unlock()
			}
		}()
	} else if state == inReduce {
		count := <-c.rTaskUnassignedCount
		c.state <- state
		if count == 0 {
			c.rTaskUnassignedCount <- 0
			reply.Task = Sleep
			return nil
		}
		taskID := <-c.reduceTasks
		c.rTaskUnassignedCount <- count - 1
		reply.Task = ReduceTask
		reply.TaskID = taskID
		reply.NMap = len(c.files)
		reply.NReduce = c.nReduce
		go func() {
			time.Sleep(time.Second * 10)
			c.reduceCompleteMutex.Lock()
			if !c.reduceComplete[taskID] {
				c.reduceCompleteMutex.Unlock()
				c.reduceTasks <- taskID
				rCount := <-c.rTaskUnassignedCount
				c.rTaskUnassignedCount <- rCount + 1
			} else {
				c.reduceCompleteMutex.Unlock()
			}
		}()
	} else if state == complete {
		c.state <- state
		reply.Task = Complete
	} else {
		c.state <- state
		reply.Task = Sleep
	}

	return nil
}

func (c *Coordinator) FinishMap(args *FinishMapArgs, reply *FinishMapReply) error {
	taskID := args.TaskID
	c.mapCompleteMutex.Lock()
	if !c.mapComplete[taskID] {
		mTaskCount := <-c.mTaskCount
		mTaskCount--
		if mTaskCount == 0 {
			<-c.state
			c.state <- inReduce
		}
		c.mTaskCount <- mTaskCount
		c.mapComplete[taskID] = true
	}
	c.mapCompleteMutex.Unlock()
	return nil
}

func (c *Coordinator) FinishReduce(args *FinishReduceArgs, reply *FinishReduceReply) error {
	taskID := args.TaskID
	c.reduceCompleteMutex.Lock()
	if !c.reduceComplete[taskID] {
		rTaskCount := <-c.rTaskCount
		rTaskCount--
		if rTaskCount == 0 {
			<-c.state
			c.state <- complete
		}
		c.rTaskCount <- rTaskCount
		c.reduceComplete[taskID] = true
	}
	c.reduceCompleteMutex.Unlock()
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
	state := <-c.state
	ret = state == complete
	c.state <- state

	return ret
}

func (c *Coordinator) init(files []string, nReduce int) {
	c.files = files
	c.nReduce = nReduce
	c.mapTasks = make(chan int, len(files))
	for i := 0; i < len(files); i++ {
		c.mapTasks <- i
	}
	c.reduceTasks = make(chan int, nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks <- i
	}
	c.state = make(chan int, 1)

	c.mapComplete = make([]bool, len(files))
	c.reduceComplete = make([]bool, nReduce)
	c.state <- inMap
	c.mTaskUnassignedCount, c.rTaskUnassignedCount = make(chan int, 1), make(chan int, 1)
	c.mTaskUnassignedCount <- len(files)
	c.rTaskUnassignedCount <- nReduce
	c.mTaskCount, c.rTaskCount = make(chan int, 1), make(chan int, 1)
	c.mTaskCount <- len(files)
	c.rTaskCount <- nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.init(files, nReduce)

	c.server()
	return &c
}
