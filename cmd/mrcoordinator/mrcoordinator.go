package main

import (
	"MapReduceDhruv/mr"
	"fmt"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	// 10 = nReduce = number of reduce tasks
	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	//This means that the map reduce job is complete
	time.Sleep(time.Second)
}
