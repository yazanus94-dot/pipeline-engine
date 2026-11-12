// Package pipeline provides a high-performance concurrent data processing framework.
// It implements fan-in/fan-out pipeline patterns with backpressure control and fault tolerance.
package pipeline

import (
	"context"
	"errors"
	"sync"
)

// StageType represents the type of pipeline stage.
type StageType int

const (
	StageSource StageType = iota
	StageTransform
	StageSink
)

// Stage represents a single processing stage in the pipeline.
type Stage struct {
	name      string
	input     chan interface{}
	output    chan interface{}
	process   ProcessFunc
 errCh    chan error
	done      chan struct{}
}

// ProcessFunc is the signature for stage processing functions.
// Source stages receive an output channel.
// Transform stages receive input and output channels.
// Sink stages receive an input channel.
type ProcessFunc func(ctx context.Context) error

// Pipeline represents a directed acyclic graph of processing stages.
type Pipeline struct {
	stages []*Stage
 errCh  chan error
 done   chan struct{}
}

// New creates a new pipeline from the given stages.
func New(stages ...*Stage) *Pipeline {
	return &Pipeline{
		stages: stages,
	 errCh:  make(chan error, len(stages)),
	 done:  make(chan struct{}),
	}
}

// Run executes the pipeline and blocks until completion or error.
func (p *Pipeline) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	// Start all stages
	for _, stage := range p.stages {
		wg.Add(1)
		go func(s *Stage) {
			defer wg.Done()
			select {
			case s.errCh <- s.process(ctx):
			case <-ctx.Done():
			}
		}(stage)
	}

	// Wait for completion or error
	go func() {
		wg.Wait()
		close(p.done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		// Check for errors
		close(p.errCh)
		for err := range p.errCh {
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// Source creates a source stage that generates data.
func Source(name string, fn func(ctx context.Context, out chan<- interface{}) error) *Stage {
	out := make(chan interface{}, 100)
	return &Stage{
		name:    name,
		input:   nil,
		output:  out,
		process: func(ctx context.Context) error {
			defer close(out)
			return fn(ctx, out)
		},
		errCh: make(chan error, 1),
		done:  make(chan struct{}),
	}
}

// Map creates a transform stage that applies a function to each item.
func Map(name string, fn func(ctx context.Context, in <-chan interface{}, out chan<- interface{}) error) *Stage {
	in := make(chan interface{}, 100)
	out := make(chan interface{}, 100)
	return &Stage{
		name:    name,
		input:   in,
		output:  out,
		process: func(ctx context.Context) error {
			defer close(out)
			return fn(ctx, in, out)
		},
		errCh: make(chan error, 1),
		done:  make(chan struct{}),
	}
}

// Sink creates a sink stage that consumes data.
func Sink(name string, fn func(ctx context.Context, in <-chan interface{}) error) *Stage {
	in := make(chan interface{}, 100)
	return &Stage{
		name:    name,
		input:   in,
		output:  nil,
		process: func(ctx context.Context) error {
			return fn(ctx, in)
		},
		errCh: make(chan error, 1),
		done:  make(chan struct{}),
	}
}

// Connect links stages in series.
func (s *Stage) Connect(next *Stage) {
	if s.output != nil && next.input != nil {
		go func() {
			for v := range s.output {
				next.input <- v
			}
			close(next.input)
		}()
	}
}

// Name returns the stage name.
func (s *Stage) Name() string {
	return s.name
}

// OutputChan returns the stage's output channel.
func (s *Stage) OutputChan() <-chan interface{} {
	return s.output
}

// InputChan returns the stage's input channel.
func (s *Stage) InputChan() chan<- interface{} {
	return s.input
}

// Err returns the stage's error channel.
func (s *Stage) Err() <-chan error {
	return s.errCh
}

// Sentinel errors
var (
	ErrPipelineClosed  = errors.New("pipeline: channel closed")
	ErrStageFailed    = errors.New("pipeline: stage processing failed")
	ErrContextCanceled = errors.New("pipeline: context canceled")
)