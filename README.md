# MapReduce Implementation

A distributed MapReduce system in Go.

## Project Structure

```
MapReduce/
├── cmd/
│   ├── mrcoordinator/    # Coordinator executable
│   └── mrworker/         # Worker executable
|   └── mrsequential/     # Sequential map reduce executable
├── mr/                   # Core MapReduce library
├── mrapps/              # MapReduce applications (plugins)
└── test-files/          # Test input files
```

## How to Run

1. **Build the plugin:**
   ```bash
   cd mrapps
   go build -buildmode=plugin wc.go
   ```

2. **Build the executables:**
   ```bash
   # Build coordinator
   go build -o mrcoordinator ./cmd/mrcoordinator
   
   # Build worker
   go build -o mrworker ./cmd/mrworker
   ```

3. **Start coordinator:**
   ```bash
   ./mrcoordinator test-files/pg-*.txt
   ```

4. **Start workers (in separate terminals):**
   ```bash
   ./mrworker mrapps/wc.so
   ```

## Alternative: Run without building

You can also run the programs directly without building:

1. **Start coordinator:**
   ```bash
   go run ./cmd/mrcoordinator test-files/pg-*.txt
   ```

2. **Start workers (in separate terminals):**
   ```bash
   go run ./cmd/mrworker mrapps/wc.so
   ```

## Output

Results are written to `mr-out-*` files in the current directory.
