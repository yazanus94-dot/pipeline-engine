package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSource_GeneratesItems(t *testing.T) {
	ctx := context.Background()
	src := Source("test", func(ctx context.Context, out chan<- interface{}) error {
		for i := 0; i < 5; i++ {
			out <- i
		}
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		src.process(ctx)
	}()

	count := 0
	for range src.OutputChan() {
		count++
	}
	wg.Wait()

	if count != 5 {
		t.Errorf("expected 5 items, got %d", count)
	}
}

func TestMap_TransformsItems(t *testing.T) {
	ctx := context.Background()
	src := Source("src", func(ctx context.Context, out chan<- interface{}) error {
		for i := 0; i < 3; i++ {
			out <- i
		}
		return nil
	})

	mapper := Map("double", func(ctx context.Context, in <-chan interface{}, out chan<- interface{}) error {
		for v := range in {
			out <- v.(int) * 2
		}
		return nil
	})

	src.Connect(mapper)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); src.process(ctx) }()
	go func() { defer wg.Done(); mapper.process(ctx) }()

	results := make([]int, 0)
	for v := range mapper.OutputChan() {
		results = append(results, v.(int))
	}
	wg.Wait()

	expected := []int{0, 2, 4}
	if len(results) != len(expected) {
		t.Errorf("expected %d results, got %d", len(expected), len(results))
	}
	for i, v := range expected {
		if results[i] != v {
			t.Errorf("results[%d] = %d, want %d", i, results[i], v)
		}
	}
}

func TestSink_ConsumesItems(t *testing.T) {
	ctx := context.Background()
	src := Source("src", func(ctx context.Context, out chan<- interface{}) error {
		for i := 0; i < 3; i++ {
			out <- i
		}
		return nil
	})

	sink := Sink("collector", func(ctx context.Context, in <-chan interface{}) error {
		for range in {
			// consume
		}
		return nil
	})

	src.Connect(sink)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); src.process(ctx) }()
	go func() { defer wg.Done(); sink.process(ctx) }()

	wg.Wait()
}

func TestPipeline_RunCompletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src := Source("gen", func(ctx context.Context, out chan<- interface{}) error {
		for i := 0; i < 10; i++ {
			out <- i
		}
		return nil
	})

	sink := Sink("print", func(ctx context.Context, in <-chan interface{}) error {
		for range in {
		}
		return nil
	})

	src.Connect(sink)

	p := New(src, sink)
	if err := p.Run(ctx); err != nil {
		t.Errorf("pipeline run failed: %v", err)
	}
}

func TestPipeline_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	src := Source("gen", func(ctx context.Context, out chan<- interface{}) error {
		for i := 0; ; i++ {
			select {
			case out <- i:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	sink := Sink("block", func(ctx context.Context, in <-chan interface{}) error {
		<-ctx.Done()
		return ctx.Err()
	})

	src.Connect(sink)

	p := New(src, sink)

	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Error("expected context canceled error")
		}
	case <-time.After(time.Second):
		t.Error("pipeline did not shut down after cancellation")
	}
}

func TestStage_NameReturnsCorrectName(t *testing.T) {
	s := Source("my-stage", func(ctx context.Context, out chan<- interface{}) error {
		return nil
	})
	if s.Name() != "my-stage" {
		t.Errorf("expected name 'my-stage', got '%s'", s.Name())
	}
}

func TestPipeline_ErrorPropagation(t *testing.T) {
	ctx := context.Background()

	src := Source("err-src", func(ctx context.Context, out chan<- interface{}) error {
		out <- 1
		return nil
	})

	badMapper := Map("err-map", func(ctx context.Context, in <-chan interface{}, out chan<- interface{}) error {
		<-in
		return ErrStageFailed
	})

	sink := Sink("err-sink", func(ctx context.Context, in <-chan interface{}) error {
		return nil
	})

	src.Connect(badMapper)
	badMapper.Connect(sink)

	p := New(src, badMapper, sink)
	err := p.Run(ctx)
	if err == nil {
		t.Error("expected error from pipeline")
	}
}