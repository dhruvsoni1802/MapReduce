package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Struct definition for Worker
type Worker struct {
	WorkerID int
}

// Struct definition for KeyValue
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key (copied from mrsequential.go)
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// This function is used to hash the key and choose the reduce task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Example RPC method to satisfy RPC registration requirements
func (w *Worker) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// This function starts the worker server that listens for RPCs from worker.go
func (w *Worker) server() (string, error) {
	rpc.Register(w)
	rpc.HandleHTTP()
	sockname := workerSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
		return "", e
	}
	go http.Serve(l, nil)
	return sockname, nil
}

// This function is an RPC handler for the worker to respond to the coordinator that it is alive
// If the worker is not alive, the coordinator will not be able to this function via RPC and hence mark it as not alive
func (w *Worker) Ping(args *PingArgs, reply *PingReply) error {
	return nil
}

// Execute a reduce task: read intermediate files, group by key, call reducef, write output
func (w *Worker) executeReduceTask(reduceTaskID int, nMap int, reducef func(string, []string) string) bool {
	// Read all intermediate files for this reduce task
	intermediate := []KeyValue{}
	
	for mapTaskID := 0; mapTaskID < nMap; mapTaskID++ {
		filename := fmt.Sprintf("reduceJobs/mr-%d-%d", mapTaskID, reduceTaskID)
		file, err := os.Open(filename)
		if err != nil {
			// File might not exist if no keys were hashed to this reduce task
			continue
		}
		
		// Read key-value pairs from this file (same format as written)
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				intermediate = append(intermediate, KeyValue{parts[0], parts[1]})
			}
		}
		file.Close()
	}
	
	fmt.Printf("Worker %d: Read %d key-value pairs for reduce task %d\n", 
		w.WorkerID, len(intermediate), reduceTaskID)
	
	// Sort by key (like in mrsequential)
	sort.Sort(ByKey(intermediate))
	
	// Create mapreducefinal directory if it doesn't exist
	err := os.MkdirAll("mapreducefinal", 0755)
	if err != nil {
		fmt.Printf("Worker %d: Cannot create mapreducefinal directory: %v\n", w.WorkerID, err)
		return false
	}
	
	// Create output file in mapreducefinal directory
	outputFilename := fmt.Sprintf("mapreducefinal/mr-out-%d", reduceTaskID)
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		fmt.Printf("Worker %d: Cannot create output file %s: %v\n", w.WorkerID, outputFilename, err)
		return false
	}
	defer outputFile.Close()
	
	// Group by key and call reducef (like in mrsequential)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		
		i = j
	}
	
	fmt.Printf("Worker %d: Completed reduce task %d, wrote to %s\n", 
		w.WorkerID, reduceTaskID, outputFilename)
	return true
}


// This function is the main function for the worker. It is called by main/mrworker.go.
func StartWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//This means we start a new worker process
	w := &Worker{}

	//We start the worker server
	sockname, err := w.server()
	if err != nil {
		log.Fatal("listen error:", err)
		os.Exit(1)
	}

	//Now we need to inform the coordinator via RPC that we have started
	registerWorkerArgs := RegisterWorkerArgs{}
	registerWorkerArgs.WorkerSocket = sockname
	registerWorkerReply := RegisterWorkerReply{}

	ok := call("Coordinator.RegisterWorker", &registerWorkerArgs, &registerWorkerReply)

	if !ok {
		fmt.Printf("RegisterWorker RPC failed - coordinator may not be available\n")
		os.Exit(1)
	}

	fmt.Printf("Worker %d registered with coordinator\n", registerWorkerReply.WorkerID)
	w.WorkerID = registerWorkerReply.WorkerID

	// Continuously request tasks from the coordinator
	for {
		
		requestArgs := RequestTaskArgs{
			WorkerID: w.WorkerID,
		}
		requestReply := RequestTaskReply{}

		ok := call("Coordinator.RequestTask", &requestArgs, &requestReply)
		if ok {
			fmt.Printf("Worker %d: TaskAvailable: %v, TaskType: %s\n", 
				w.WorkerID, requestReply.TaskAvailable, requestReply.TaskType)
			
			// If no task available, continue the loop
			if !requestReply.TaskAvailable {
				time.Sleep(5 * time.Second)
				continue
			}
			
			// Handle different task types
			if requestReply.TaskType == "map" {
				fmt.Printf("Worker %d: Received map task %d for file %s\n", 
					w.WorkerID, requestReply.TaskID, requestReply.Filename)
				
				// Simple map task execution - just like mrsequential
				file, err := os.Open(requestReply.Filename)
				if err != nil {
					fmt.Printf("Worker %d: Cannot open file %s: %v\n", w.WorkerID, requestReply.Filename, err)
				} else {
					content, err := ioutil.ReadAll(file)
					file.Close()
					if err != nil {
						fmt.Printf("Worker %d: Cannot read file %s: %v\n", w.WorkerID, requestReply.Filename, err)
					} else {
						kva := mapf(requestReply.Filename, string(content))
						fmt.Printf("Worker %d: Processed %d key-value pairs from %s\n", 
							w.WorkerID, len(kva), requestReply.Filename)
						
						// Create reduceJobs directory if it doesn't exist
						err = os.MkdirAll("reduceJobs", 0755)
						if err != nil {
							fmt.Printf("Worker %d: Cannot create reduceJobs directory: %v\n", w.WorkerID, err)
						} else {
							// Create intermediate files for each reduce task
							intermediateFiles := make([]*os.File, requestReply.NReduce)
							for i := 0; i < requestReply.NReduce; i++ {
								filename := fmt.Sprintf("reduceJobs/mr-%d-%d", requestReply.TaskID, i)
								intermediateFiles[i], err = os.Create(filename)
								if err != nil {
									fmt.Printf("Worker %d: Cannot create file %s: %v\n", w.WorkerID, filename, err)
									// Close already opened files
									for j := 0; j < i; j++ {
										intermediateFiles[j].Close()
									}
									break
								}
							}
							
							// Write key-value pairs to appropriate intermediate files
							for _, kv := range kva {
								reduceTask := ihash(kv.Key) % requestReply.NReduce
								fmt.Fprintf(intermediateFiles[reduceTask], "%v %v\n", kv.Key, kv.Value)
							}
							
							// Close all intermediate files
							for i := 0; i < requestReply.NReduce; i++ {
								if intermediateFiles[i] != nil {
									intermediateFiles[i].Close()
								}
							}
						}
						
						// Report task completion
						completeArgs := TaskCompleteArgs{
							WorkerID: w.WorkerID,
							TaskType: "map",
							TaskID:   requestReply.TaskID,
						}
						completeReply := TaskCompleteReply{}
						call("Coordinator.CompleteTask", &completeArgs, &completeReply)
					}
				}
				
			} else if requestReply.TaskType == "reduce" {
				fmt.Printf("Worker %d: Received reduce task %d\n", 
					w.WorkerID, requestReply.TaskID)
				
				// Execute reduce task
				success := w.executeReduceTask(requestReply.TaskID, requestReply.NReduce, reducef)
				if success {
					// Report task completion to coordinator
					completeArgs := TaskCompleteArgs{
						WorkerID: w.WorkerID,
						TaskType: "reduce",
						TaskID:   requestReply.TaskID,
					}
					completeReply := TaskCompleteReply{}
					
					ok := call("Coordinator.CompleteTask", &completeArgs, &completeReply)
					if ok {
						fmt.Printf("Worker %d: Successfully completed reduce task %d\n", w.WorkerID, requestReply.TaskID)
					} else {
						fmt.Printf("Worker %d: Failed to report completion of reduce task %d\n", w.WorkerID, requestReply.TaskID)
					}
				} else {
					fmt.Printf("Worker %d: Failed to execute reduce task %d\n", w.WorkerID, requestReply.TaskID)
				}
				
			} else if requestReply.TaskType == "done" {
				fmt.Printf("Worker %d: All tasks completed, exiting\n", w.WorkerID)
				break
			}
			
		} else {
			fmt.Printf("Worker %d: RequestTask RPC failed - coordinator may have exited\n", w.WorkerID)
			fmt.Printf("Worker %d: Exiting\n", w.WorkerID)
			break
		}
		
		// Wait 5 seconds before requesting the next task
		time.Sleep(5 * time.Second)
	}
}

// This function is an example function to show how to make an RPC call to the coordinator.
// The RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// Declare an argument structure.
	args := ExampleArgs{}

	// Fill in the argument(s).
	args.X = 99

	// Declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// The "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// This function sends an RPC request to the coordinator, waits for the response.
// Usually returns true.
// Returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// Handle connection refused gracefully - coordinator may have exited
		fmt.Printf("Cannot connect to coordinator: %v\n", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

