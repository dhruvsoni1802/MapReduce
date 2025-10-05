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

// Task status constants
const (
	TaskPending = "pending"
	TaskInProgress = "in_progress"
	TaskCompleted = "completed"
)

// Map task information
type MapTask struct {
	TaskID   int
	Filename string
	Status   string
	WorkerID int // Which worker is assigned to this task (-1 if not assigned)
}

// Reduce task information
type ReduceTask struct {
	TaskID   int
	Status   string
	WorkerID int // Which worker is assigned to this task (-1 if not assigned)
}

// The coordinator is responsible for assigning map and reduce tasks to the workers
type Coordinator struct {
	mu sync.Mutex // Mutex to protect concurrent access to coordinator state
	
	nReduceperMap int // Coordinator needs to keep track of the number of reduce tasks per map task. This will not change.
	nMap int // Coordinator needs to keep track of the number of map tasks. This will not change.
	nWorkers int // Coordinator needs to keep track of the number of workers. This will keep changing as new workers are added or removed.
	nWorkersAlive int // Coordinator needs to keep track of the number of workers amongst total workers that are alive.
	workerSockets map[int]string // Map worker ID to their socket address
	workersAlive map[int]bool // Map worker ID to their alive status
	
	// Task tracking
	mapTasks []MapTask // Track all map tasks
	reduceTasks []ReduceTask // Track all reduce tasks
	mapTasksCompleted int // Count of completed map tasks
	reduceTasksCompleted int // Count of completed reduce tasks
}

//RPC handlers for the worker to call.
// 1. RPC handler to register a new worker when it is started
// It gives a unique ID to each worker as a reply
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.nWorkers++
	c.nWorkersAlive++
	reply.WorkerID = c.nWorkers
	
	// Store the worker's socket address in the map
	c.workerSockets[reply.WorkerID] = args.WorkerSocket
	c.workersAlive[reply.WorkerID] = true

	fmt.Printf("Number of workers: %d\n", c.nWorkers)
	
	return nil
}

// Helper function to find the next available map task
func (c *Coordinator) findNextMapTask() *MapTask {
	for i := range c.mapTasks {
		if c.mapTasks[i].Status == TaskPending {
			return &c.mapTasks[i]
		}
	}
	return nil
}

// Helper function to find the next available reduce task
func (c *Coordinator) findNextReduceTask() *ReduceTask {
	for i := range c.reduceTasks {
		if c.reduceTasks[i].Status == TaskPending {
			return &c.reduceTasks[i]
		}
	}
	return nil
}

// Helper function to reassign tasks from a crashed worker back to pending
func (c *Coordinator) reassignTasksFromWorker(workerID int) {
	// Reassign map tasks from this worker
	for i := range c.mapTasks {
		if c.mapTasks[i].WorkerID == workerID && c.mapTasks[i].Status == TaskInProgress {
			c.mapTasks[i].Status = TaskPending
			c.mapTasks[i].WorkerID = -1
			fmt.Printf("Reassigned map task %d (file: %s) from crashed worker %d back to pending\n", 
				c.mapTasks[i].TaskID, c.mapTasks[i].Filename, workerID)
		}
	}
	
	// Reassign reduce tasks from this worker
	for i := range c.reduceTasks {
		if c.reduceTasks[i].WorkerID == workerID && c.reduceTasks[i].Status == TaskInProgress {
			c.reduceTasks[i].Status = TaskPending
			c.reduceTasks[i].WorkerID = -1
			fmt.Printf("Reassigned reduce task %d from crashed worker %d back to pending\n", 
				c.reduceTasks[i].TaskID, workerID)
		}
	}
}

// 2. RPC handler for workers to request tasks
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// First, try to assign a map task if map phase is not complete
	if c.mapTasksCompleted < c.nMap {
		mapTask := c.findNextMapTask()
		if mapTask != nil {
			// Assign this map task to the requesting worker
			mapTask.Status = TaskInProgress
			mapTask.WorkerID = args.WorkerID

			reply.TaskAvailable = true
			reply.TaskType = "map"
			reply.TaskID = mapTask.TaskID
			reply.Filename = mapTask.Filename
			reply.NReduce = c.nReduceperMap
			
			fmt.Printf("Assigned map task %d (file: %s) to worker %d\n", 
				mapTask.TaskID, mapTask.Filename, args.WorkerID)
			return nil
		}
	}
	
		// If map phase is complete, try to assign a reduce task
	if c.mapTasksCompleted == c.nMap && c.reduceTasksCompleted < c.nReduceperMap {
		reduceTask := c.findNextReduceTask()
		if reduceTask != nil {
			// Assign this reduce task to the requesting worker
			reduceTask.Status = TaskInProgress
			reduceTask.WorkerID = args.WorkerID
			
			reply.TaskAvailable = true
			reply.TaskType = "reduce"
			reply.TaskID = reduceTask.TaskID
			reply.Filename = ""
			reply.NReduce = c.nMap  // Pass number of map tasks, not reduce tasks
			
			fmt.Printf("Assigned reduce task %d to worker %d\n", 
				reduceTask.TaskID, args.WorkerID)
			return nil
		}
	}
	
	// No tasks available
	reply.TaskAvailable = false
	reply.TaskType = "done"
	reply.TaskID = 0
	reply.Filename = ""
	reply.NReduce = c.nReduceperMap
	return nil
}

// 3. RPC handler for workers to report task completion
func (c *Coordinator) CompleteTask(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if args.TaskType == "map" {
		// Find the map task and mark it as completed
		if args.TaskID >= 0 && args.TaskID < len(c.mapTasks) {
			c.mapTasks[args.TaskID].Status = TaskCompleted
			c.mapTasks[args.TaskID].WorkerID = -1 // Clear worker assignment
			c.mapTasksCompleted++
			
			fmt.Printf("Map task %d (file: %s) completed by worker %d\n", 
				args.TaskID, c.mapTasks[args.TaskID].Filename, args.WorkerID)
			
			reply.Success = true
			reply.Message = "Map task completed successfully"
		} else {
			reply.Success = false
			reply.Message = "Invalid map task ID"
		}
	} else if args.TaskType == "reduce" {
		// Find the reduce task and mark it as completed
		if args.TaskID >= 0 && args.TaskID < len(c.reduceTasks) {
			c.reduceTasks[args.TaskID].Status = TaskCompleted
			c.reduceTasks[args.TaskID].WorkerID = -1 // Clear worker assignment
			c.reduceTasksCompleted++
			
			fmt.Printf("Reduce task %d completed by worker %d\n", 
				args.TaskID, args.WorkerID)
			
			reply.Success = true
			reply.Message = "Reduce task completed successfully"
		} else {
			reply.Success = false
			reply.Message = "Invalid reduce task ID"
		}
	} else {
		reply.Success = false
		reply.Message = "Unknown task type"
	}
	
	return nil
}

// This function makes an RPC call to a specific worker
func (c *Coordinator) callWorker(workerID int, rpcname string, args interface{}, reply interface{}) bool {
	sockname, exists := c.workerSockets[workerID]
	if !exists {
		fmt.Printf("Worker %d not found\n", workerID)
		return false
	}
	
	client, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		fmt.Printf("Error dialing worker %d: %v\n", workerID, err)
		return false
	}
	defer client.Close()
	
	err = client.Call(rpcname, args, reply)
	if err != nil {
		fmt.Printf("Error calling %s on worker %d: %v\n", rpcname, workerID, err)
		return false
	}
	
	return true
}

// This function starts the coordinator erver that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// This function is a go routine running in background that pings all the workers via RPC to check if they are alive
// If a worker is not alive, the nworkeralive is decremented and the workersAlive map is update to false
func (c *Coordinator) pingWorkers() {
	for{
		for workerID,alive := range c.workersAlive {
			pingArgs := &PingArgs{}
			pingReply := &PingReply{}
			if c.callWorker(workerID, "Worker.Ping", pingArgs, pingReply) && alive {
				fmt.Printf("Worker %d is alive\n", workerID)
				c.workersAlive[workerID] = true
			} else {
				// Only decrement if the worker was previously alive
				if alive {
					c.nWorkersAlive--
					// Reassign tasks from this crashed worker
					c.reassignTasksFromWorker(workerID)
				}
				c.workersAlive[workerID] = false
				fmt.Printf("Worker %d is not alive\n", workerID)
			}
		}
		
		// Sleep for 10 seconds before the next ping cycle
		time.Sleep(10 * time.Second)
	}
}

// This function is called periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// The job is done when all reduce tasks are completed
	return c.reduceTasksCompleted == c.nReduceperMap
}

// This function creates a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}
	c.nReduceperMap = nReduce
	c.nMap = len(files)
	c.nWorkers = 0
	c.nWorkersAlive = 0
	c.workerSockets = make(map[int]string)
	c.workersAlive = make(map[int]bool)
	
	// Initialize task tracking counters
	c.mapTasksCompleted = 0
	c.reduceTasksCompleted = 0
	
	// Initialize map tasks - create one MapTask for each input file
	c.mapTasks = make([]MapTask, len(files))
	for i, filename := range files {
		c.mapTasks[i] = MapTask{
			TaskID:   i,
			Filename: filename,
			Status:   TaskPending,
			WorkerID: -1, // Not assigned to any worker yet
		}
	}
	
	// Initialize reduce tasks
	c.reduceTasks = make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{
			TaskID:   i,
			Status:   TaskPending,
			WorkerID: -1, // Not assigned to any worker yet
		}
	}

	// Debugging to print the number of input splits to read and number of reduce tasks to perform per map task
	fmt.Printf("Number of input splits to read: %d\n", c.nMap)
	fmt.Printf("Number of reduce tasks to perform per map task: %d\n", c.nReduceperMap)
	fmt.Printf("Initialized %d map tasks and %d reduce tasks\n", len(c.mapTasks), len(c.reduceTasks))

	// We now start the server
	c.server()

	// We now start the go routine to ping all the workers
	go c.pingWorkers()

	return &c
}
