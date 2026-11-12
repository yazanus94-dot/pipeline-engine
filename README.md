# Pipeline Engine

A high-performance, concurrent data processing framework for Go. Built on fan-in/fan-out pipeline patterns with backpressure control, batch processing, and fault-tolerant stage isolation.

![Go](https://img.shields.io/badge/Go-1.21-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Testing](https://img.shields.io/badge/Tests-Go-test-green)

## Overview

Pipeline Engine provides a declarative, composable API for building data pipelines. Each stage runs in its own goroutine with buffered channels providing natural backpressure. Stages can be combined in series, parallel, or fan-out/fan-in configurations. The framework handles worker lifecycle, error propagation, and graceful shutdown automatically.

## Key Features

- **Fan-in/Fan-out**: Merge multiple inputs or distribute work across workers
- **Backpressure**: Buffered channels prevent memory exhaustion under load
- **Batch Processing**: Group items for efficient bulk operations
- **Stage Isolation**: Each stage runs independently with its own error handling
- **Graceful Shutdown**: Context-based cancellation propagates through all stages
- **Zero-Allocation Paths**: Minimize garbage collector pressure in hot paths
- **Composable Stages**: Map, Filter, Reduce, Batch, Window, Merge, Split

## Architecture

```
Source → Stage 1 → Stage 2 → ... → Stage N → Sink
              ↓          ↓
         (parallel)  (parallel)
```

Each stage is a goroutine with an input channel, processing function, and output channel. The pipeline forms a directed acyclic graph of data flow.

## Installation

```bash
go get github.com/yourusername/pipeline-engine
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    "github.com/yourusername/pipeline-engine"
)

func main() {
    ctx := context.Background()
    
    // Create a pipeline: generate → square → print
    p := pipeline.New(
        pipeline.Generator(generateNumbers),
        pipeline.Map(square),
        pipeline.Sink(printResult),
    )
    
    if err := p.Run(ctx); err != nil {
        log.Fatal(err)
    }
}

func generateNumbers(ctx context.Context, out chan<- int) error {
    for i := 0; i < 10; i++ {
        select {
        case out <- i:
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    return nil
}

func square(ctx context.Context, in <-chan int, out chan<- int) error {
    for v := range in {
        select {
        case out <- v * v:
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    return nil
}

func printResult(ctx context.Context, in <-chan int) error {
    for v := range in {
        fmt.Println(v)
    }
    return nil
}
```

## Testing

```bash
go test -v ./...
go test -race -bench=. ./...
```

## License

MIT