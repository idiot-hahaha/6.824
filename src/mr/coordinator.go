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

type TaskID int
type WorkerID int

type Coordinator struct {
	// Your definitions here.
	nReduce             int
	nMap                int
	state               chan int
	files               []string
	Tasks               chan TaskID
	unassignedTaskCount chan int
	taskCount           chan int
	mapComplete         []bool
	reduceComplete      []bool
	workerTaskID        map[WorkerID]TaskID
	workerAlive         map[WorkerID]bool
	//mutex               sync.Mutex
	mapCompleteMutex    sync.Mutex
	reduceCompleteMutex sync.Mutex
	workerTaskIDMutex   sync.Mutex
	workerAliveMutex    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	state := <-c.state
	if state == inMap {
		count := <-c.unassignedTaskCount
		c.state <- state
		if count == 0 {
			c.unassignedTaskCount <- 0
			reply.Task = Sleep
			return nil
		}
		taskID := <-c.Tasks
		c.unassignedTaskCount <- count - 1
		reply.Task = MapTask
		reply.TaskID = taskID
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.Inames = []string{c.files[taskID]}
		c.workerTaskIDMutex.Lock()
		c.workerTaskID[args.ID] = taskID
		c.workerTaskIDMutex.Unlock()
	} else if state == inReduce {
		count := <-c.unassignedTaskCount
		c.state <- state
		if count == 0 {
			c.unassignedTaskCount <- 0
			reply.Task = Sleep
			return nil
		}
		taskID := <-c.Tasks
		c.unassignedTaskCount <- count - 1
		reply.Task = ReduceTask
		reply.TaskID = taskID
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		c.workerTaskIDMutex.Lock()
		c.workerTaskID[args.ID] = taskID
		c.workerTaskIDMutex.Unlock()
	} else if state == complete {
		c.state <- state
		reply.Task = Complete
		return nil
	} else {
		c.state <- state
		reply.Task = Sleep
		return nil
	}

	return nil
}

func (c *Coordinator) FinishMap(args *FinishMapArgs, reply *FinishMapReply) error {
	taskID := args.TaskID
	c.mapCompleteMutex.Lock()
	if !c.mapComplete[taskID] {
		c.mapCompleteMutex.Unlock()
		mTaskCount := <-c.taskCount
		mTaskCount--
		if mTaskCount == 0 {
			<-c.state
			c.state <- inReduce
			c.Tasks = make(chan TaskID, c.nReduce)
			for i := 0; i < c.nReduce; i++ {
				c.Tasks <- TaskID(i)
			}
			c.taskCount <- c.nReduce
			<-c.unassignedTaskCount
			c.unassignedTaskCount <- c.nReduce
			c.workerTaskIDMutex.Lock()
			c.workerTaskID = map[WorkerID]TaskID{}
			c.workerTaskIDMutex.Unlock()
			return nil
		}
		c.taskCount <- mTaskCount
		c.mapComplete[taskID] = true
	}
	//c.mutex.Unlock()
	return nil
}

func (c *Coordinator) FinishReduce(args *FinishReduceArgs, reply *FinishReduceReply) error {
	taskID := args.TaskID
	c.reduceCompleteMutex.Lock()
	if !c.reduceComplete[taskID] {
		rTaskCount := <-c.taskCount
		rTaskCount--
		if rTaskCount == 0 {
			<-c.state
			c.state <- complete
		}
		c.taskCount <- rTaskCount
		c.reduceComplete[taskID] = true
	}
	c.reduceCompleteMutex.Unlock()
	return nil
}

func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) error {
	c.workerAliveMutex.Lock()
	c.workerAlive[args.ID] = true
	c.workerAliveMutex.Unlock()
	return nil
}
func (c *Coordinator) Link(args *LinkArgs, reply *LinkReply) error {
	workerID := args.ID
	c.workerAliveMutex.Lock()
	c.workerAlive[workerID] = true
	c.workerAliveMutex.Unlock()

	go func() {
		for {
			time.Sleep(time.Second)
			c.workerAliveMutex.Lock()
			if !c.workerAlive[workerID] {
				delete(c.workerAlive, workerID)
				c.workerAliveMutex.Unlock()
				c.workerTaskIDMutex.Lock()
				c.Tasks <- c.workerTaskID[workerID]
				delete(c.workerTaskID, workerID)
				c.workerTaskIDMutex.Unlock()
				unassignedTaskCount := <-c.unassignedTaskCount
				c.unassignedTaskCount <- unassignedTaskCount + 1
				return
			}
			c.workerAlive[workerID] = false
			c.workerAliveMutex.Unlock()
			//select {
			//case <-c.workerAlive[workerID]:
			//	continue
			//case <-time.After(time.Second):
			//	c.Tasks <- c.workerTaskID[workerID]
			//	c.mutex.Lock()
			//	delete(c.workerAlive, workerID)
			//	//c.workerAliveMutex.Unlock()
			//	//c.workerTaskIDMutex.Lock()
			//	delete(c.workerTaskID, workerID)
			//	c.mutex.Unlock()
			//	unassignedTaskCount := <-c.unassignedTaskCount
			//	c.unassignedTaskCount <- unassignedTaskCount + 1
			//	return
			//}
		}
	}()
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

func (c *Coordinator) initForMap(files []string, nReduce int) {
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.state = make(chan int, 1)
	c.Tasks = make(chan TaskID, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.Tasks <- TaskID(i)
	}
	c.mapComplete = make([]bool, c.nMap)
	c.reduceComplete = make([]bool, c.nReduce)
	c.state <- inMap
	c.unassignedTaskCount = make(chan int, 1)
	c.unassignedTaskCount <- c.nMap
	c.taskCount = make(chan int, 1)
	c.taskCount <- c.nMap
	c.workerTaskID = map[WorkerID]TaskID{}
	c.workerAlive = map[WorkerID]bool{}
}

func (c *Coordinator) initForReduce() {
	<-c.state
	c.state <- inReduce
	c.Tasks = make(chan TaskID, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.Tasks <- TaskID(i)
	}
	c.workerTaskIDMutex.Lock()
	c.workerTaskID = map[WorkerID]TaskID{}
	c.workerTaskIDMutex.Unlock()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.initForMap(files, nReduce)

	c.server()
	return &c
}
