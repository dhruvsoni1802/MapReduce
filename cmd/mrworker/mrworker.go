package main

import (
	"MapReduceDhruv/mr"
	"log"
	"os"
	"plugin"
)

func main() {
	runWorker()
}

func runWorker() { // Renamed from 'main' to resolve the "main redeclared in this block" error.
	if len(os.Args) != 2 {
		// Replaced fmt.Fprintf and os.Exit with log.Fatalf as per logging preferences.
		log.Fatalf("Usage: mrworker xxx.so\n")
	}

	mapf, reducef := loadPlugin(os.Args[1])

	mr.StartWorker(mapf, reducef)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}


