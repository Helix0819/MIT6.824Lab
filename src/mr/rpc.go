package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskType string

const (
	Map    TaskType = "map"
	Reduce TaskType = "reduce"
	Done   TaskType = "done"
)

type Phase string

const (
	MapPhase    Phase = "map"
	ReducePhase Phase = "reduce"
	finish      Phase = "finish"
)

type Status string

const (
	Idle    Status = "idle"
	Running Status = "running"
	Finish  Status = "finish"
)

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

type Task struct {
	File      string
	Type      TaskType
	WorkerId  string
	StartTime time.Time
	Status    Status
	TaskId    int
	NReduce   int
}

type TaskRequest struct {
	WorkerId string
	Type     TaskType
}

type TaskResponse struct {
	Task   *Task
	IsDone bool
	Type   TaskType
}

type MapTaskDoneRequest struct {
	WorkerId string
	Files    []string
}

type MapTaskDoneResponse struct {
}

type ReduceTaskDoneRequest struct {
	WorkerId string
	Files    string
}

type ReduceTaskDoneResponse struct {
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

const mapfilepath string = "./"
const reducefilepath string = "./"
