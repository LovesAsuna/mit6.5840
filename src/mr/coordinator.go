package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type State int

const (
	MAP State = iota
	REDUCE
	DONE
	NO_TASK
	TIMEOUT = 10 * time.Second
)

type Task struct {
	Id       int
	NMap     int
	NReduce  int
	FileName string
	DeadLine int64
	State    State
}

type Coordinator struct {
	// Your definitions here.
	mutex     sync.Mutex
	nMap      int
	nReduce   int
	state     State
	taskQueue chan *Task
	taskMap   map[int]*Task
}

// Your code here -- RPC handlers for the worker to call.

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

	ret = c.state == DONE
	return ret
}

func (c *Coordinator) background() {
	for {
		c.mutex.Lock()
		if len(c.taskMap) > 0 {
			c.checkTimeOut()
		} else {
			c.transferState()
		}
		c.mutex.Unlock()
	}
}

func (c *Coordinator) checkTimeOut() {
	for _, task := range c.taskMap {
		if task.DeadLine != -1 && time.Now().Unix() > task.DeadLine {
			// 超时
			fmt.Printf("task of id %v with file %v, state %v, timeout at %v\n", task.Id, task.FileName, task.State, task.DeadLine)
			task.DeadLine = -1
			c.taskQueue <- task
		}
	}
}

func (c *Coordinator) transferState() {
	switch c.state {
	case MAP:
		c.state = REDUCE
		c.taskMap = make(map[int]*Task)
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Id:       i,
				State:    REDUCE,
				DeadLine: -1,
				NMap:     c.nMap,
				NReduce:  c.nReduce,
			}
			c.taskQueue <- &task
			c.taskMap[i] = &task
		}
	case REDUCE:
		c.state = DONE
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Id:       i,
				State:    DONE,
				DeadLine: -1,
				NMap:     c.nMap,
				NReduce:  c.nReduce,
			}
			c.taskQueue <- &task
			c.taskMap[i] = &task
		}
	case DONE:
		fmt.Println("Coordinator exit")
		os.Exit(0)
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce
	c.taskQueue = make(chan *Task, int(math.Max(float64(nReduce), float64(len(files)))))
	c.taskMap = make(map[int]*Task)
	c.mutex = sync.Mutex{}
	c.state = MAP
	for i, fileName := range files {
		task := Task{
			Id:       i,
			NMap:     c.nMap,
			NReduce:  c.nReduce,
			FileName: fileName,
			DeadLine: -1,
		}
		c.taskQueue <- &task
		c.taskMap[i] = &task
	}

	go c.background()
	c.server()
	return &c
}

func (c *Coordinator) FetchTask(request *FetchTaskRequest, response *FetchTaskResponse) error {
	if len(c.taskMap) > 0 {
		task := <-c.taskQueue
		task.DeadLine = time.Now().Add(time.Duration(TIMEOUT)).Unix()
		response.Task = *task
	} else {
		response.Task = Task{
			State: NO_TASK,
		}
	}
	return nil
}

func (c *Coordinator) TaskDone(request *TaskDoneRequest, response *TaskDoneResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	fmt.Printf("delete task of id %v, state: %v\n", request.Id, c.taskMap[request.Id].State)
	delete(c.taskMap, request.Id)
	return nil
}
