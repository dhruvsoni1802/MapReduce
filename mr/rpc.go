package mr

import (
	"os"
	"strconv"
)

// Struct definitions for Arguments and replies

// 1. RegisterWorkerArgs and RegisterWorkerReply
type RegisterWorkerArgs struct {
	WorkerSocket string // Socket address of the worker
}

type RegisterWorkerReply struct {
	WorkerID int 	// This is the unique ID assigned to the worker	
}

// 2. AssignTaskArgs and AssignTaskReply
type AssignTaskArgs struct {
	TaskType string // "map" or "reduce"
	TaskID   int    // Unique task identifier
	Filename string // Input file for map tasks
}

type AssignTaskReply struct {
	Accepted bool   // Whether worker accepted the task
	Message  string // Status message
}

// 3. Task Complete Args from worker and Task Complete Reply from coordinator
type TaskCompleteArgs struct {
	WorkerID int    // Worker identifier
	TaskType string // "map" or "reduce"
	TaskID   int    // Task identifier
	Output   string // Output file path
}

type TaskCompleteReply struct {
	Success bool   // Whether task completion was acknowledged
	Message string // Status message
}

// 4. ExampleArgs and ExampleReply for testing RPC calls
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// 5. PingArgs and PingReply for checking if the worker is alive. Both are empty structs
type PingArgs struct {
}

type PingReply struct {
}

// 6. RequestTaskArgs and RequestTaskReply for workers to request tasks from coordinator
type RequestTaskArgs struct {
	WorkerID int // Worker identifier requesting a task
}

type RequestTaskReply struct {
	TaskAvailable bool   // Whether there's a task available
	TaskType      string // "map", "reduce", or "done"
	TaskID        int    // Unique task identifier
	Filename      string // Input file for map tasks
	NReduce       int    // Number of reduce tasks (needed for map tasks)
}

// This function creates a unique-ish UNIX-domain socket name in /var/tmp
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// This function creates a unique UNIX-domain socket name for a worker using process ID
func workerSock() string {
	s := "/var/tmp/5840-worker-"
	s += strconv.Itoa(os.Getuid())
	s += "-"
	s += strconv.Itoa(os.Getpid())
	return s
}
