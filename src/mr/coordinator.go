package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	tasks map[TaskType][]*Task

	nMap    int
	nReduce int

	phase Phase
	mu    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(req *TaskRequest, resp *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case MapPhase:
		resp.Task = findAvailableTask(c.tasks[Map], req.WorkerId)
		resp.Type = Map
		if c.judgeMapDone() {
			fmt.Print("Map tasks done\n")
			c.phase = ReducePhase
			c.generateReduceTasks()
		}
	case ReducePhase:
		fmt.Println("Assign reduce tasks")
		resp.Task = findAvailableTask(c.tasks[Reduce], req.WorkerId)
		resp.Type = Reduce
		if c.judgeReduceDone() {
			fmt.Print("Reduce tasks done\n")
			c.phase = finish
		}
	case finish:
		task := Task{Type: Done}
		resp.Task = &task
		resp.IsDone = true
		c.Done()
	}
	return nil
}

func findAvailableTask(tasks []*Task, workerID string) *Task {
	for _, t := range tasks {
		if t.Status == Idle || (t.Status == Running && time.Now().Sub(t.StartTime) > 10*time.Second) {
			t.StartTime = time.Now()
			t.Status = Running
			t.WorkerId = workerID
			return t
		}
	}
	// fmt.Println("No task available, wait for 1 second")
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
	if c.phase == finish {
		ret = true
	}

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
	c.phase = MapPhase
	c.tasks = make(map[TaskType][]*Task)

	for i, file := range files {
		task := Task{
			Type:      Map,
			TaskId:    i,
			Status:    Idle,
			StartTime: time.Time{},
			NReduce:   nReduce,
			File:      file,
		}
		c.tasks[Map] = append(c.tasks[Map], &task)
	}
	c.server()
	return &c
}

func (c *Coordinator) MapTasksDone(req *MapTaskDoneRequest, resp *MapTaskDoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range c.tasks[Map] {
		if t.WorkerId == req.WorkerId && t.Status == Running {
			t.Status = Finish
		}
	}
	// fmt.Printf("Map task done by worker %v\n", req.WorkerId)
	return nil
}

func (c *Coordinator) ReduceTasksDone(req *ReduceTaskDoneRequest, resp *ReduceTaskDoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range c.tasks[Reduce] {
		if t.WorkerId == req.WorkerId {
			t.Status = Finish
		}
	}
	// fmt.Printf("Reduce task done by worker %v\n", req.WorkerId)
	return nil
}

func (c *Coordinator) judgeMapDone() bool {
	for _, t := range c.tasks[Map] {
		if t.Status != Finish {
			return false
		}
	}
	return true
}

func (c *Coordinator) judgeReduceDone() bool {
	for _, t := range c.tasks[Reduce] {
		if t.Status != Finish {
			return false
		}
	}
	return true
}

func (c *Coordinator) generateReduceTasks() {
	// c.mu.Lock()
	// defer c.mu.Unlock()
	fmt.Println("Generate reduce tasks")
	for i := 0; i < c.nReduce; i++ {
		task := Task{
			Type:      Reduce,
			TaskId:    i,
			Status:    Idle,
			StartTime: time.Time{},
			NReduce:   c.nReduce,
			File:      fmt.Sprintf("%vmr-*-%v", mapfilepath, i),
		}
		c.tasks[Reduce] = append(c.tasks[Reduce], &task)
		//fmt.Println("Generate reduce task ", i, " \n")
	}
}
