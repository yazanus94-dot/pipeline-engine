package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkPipeline_Small(b *testing.B) {
	src := Source("gen", func(ctx context.Context, out chan<- interface{}) error {
		for i := 0; i < 100; i++ {
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

	sink := Sink("collect", func(ctx context.Context, in <-chan interface{}) error {
		for range in {
		}
		return nil
	})

	src.Connect(mapper)
	mapper.Connect(sink)

	p := New(src, mapper, sink)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Run(context.Background())
	}
}

func BenchmarkPipeline_Large(b *testing.B) {
	src := Source("gen", func(ctx context.Context, out chan<- interface{}) error {
		for i := 0; i < 10000; i++ {
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

	sink := Sink("collect", func(ctx context.Context, in <-chan interface{}) error {
		for range in {
		}
		return nil
	})

	src.Connect(mapper)
	mapper.Connect(sink)

	p := New(src, mapper, sink)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Run(context.Background())
	}
}

func ExamplePipeline() {
	ctx := context.Background()
	src := Source("gen", func(ctx context.Context, out chan<- interface{}) error {
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

	sink := Sink("print", func(ctx context.Context, in <-chan interface{}) error {
		for v := range in {
			fmt.Println(v)
		}
		return nil
	})

	src.Connect(mapper)
	mapper.Connect(sink)

	p := New(src, mapper, sink)
	_ = p.Run(ctx)
	// Output: 0
	// 2
	// 4
}

func ExamplePipeline_ContextTimeout() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

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
	err := p.Run(ctx)
	fmt.Println(err != nil)
	// Output: true
}