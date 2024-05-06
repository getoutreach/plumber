package plumber_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getoutreach/plumber"
	"gotest.tools/v3/assert"
)

func reportingRunner(name string, fce ...func()) plumber.RunnerCloser {
	return plumber.GracefulRunner(func(ctx context.Context, ready plumber.ReadyFunc) error {
		ready()
		fmt.Println("runner", name, "started")
		for _, f := range fce {
			f()
		}
		return nil
	}, func(ctx context.Context) error {
		fmt.Println("runner", name, "closed")
		return nil
	})
}

// nolint: unparam //Why: not yet
func reportingBlockingRunner(name string, fce ...func()) plumber.RunnerCloser {
	return plumber.GracefulRunner(func(ctx context.Context, ready plumber.ReadyFunc) error {
		ready()
		fmt.Println("runner", name, "started")
		for _, f := range fce {
			f()
		}
		<-ctx.Done()
		return nil
	}, func(ctx context.Context) error {
		fmt.Println("runner", name, "closed")
		return nil
	})
}

func erroringRunner(name string) plumber.RunnerCloser {
	return plumber.SimpleRunner(func(ctx context.Context) error {
		return errors.New("runner " + name + " failed")
	})
}

// nolint: unused //Why: not yet
func erroringCloser(name string) plumber.RunnerCloser {
	return plumber.Closer(func(ctx context.Context) error {
		return errors.New("runner " + name + " failed")
	})
}

func ExamplePipeline() {
	ctx := context.Background()

	err := plumber.Start(ctx,
		plumber.Pipeline(
			reportingBlockingRunner("runner 1"),
			reportingBlockingRunner("runner 2"),
			reportingBlockingRunner("runner 3"),
		),
		plumber.TTL(10*time.Millisecond),
	)
	if err != nil {
		panic(err)
	}
	// Output:
	// runner runner 1 started
	// runner runner 2 started
	// runner runner 3 started
	// runner runner 3 closed
	// runner runner 2 closed
	// runner runner 1 closed
}

func TestPipelineErrors(t *testing.T) {
	ctx := context.Background()

	err := plumber.Start(ctx,
		plumber.Pipeline(
			erroringRunner("runner 1"),
			erroringRunner("runner 2"),
			erroringRunner("runner 3"),
		),
		plumber.TTL(100*time.Millisecond),
		func(o *plumber.Options) {
			o.Closer(func(ctx context.Context) error {
				return errors.New("Closer error")
			})
		},
	)
	assert.ErrorContains(t, err, "runner runner 1 failed")
	assert.ErrorContains(t, err, "runner runner 2 failed")
	assert.ErrorContains(t, err, "runner runner 3 failed")
	assert.ErrorContains(t, err, "Closer error")
}

func TestPipelineSignalerClosing(t *testing.T) {
	ctx := context.Background()

	signaler := plumber.NewErrorSignaler()

	err := plumber.Start(ctx,
		plumber.Pipeline(
			reportingRunner("runner 1", func() {
				// Let other job to start as well
				time.Sleep(10 * time.Millisecond)
			}),
			plumber.Runner(func(ctx context.Context, ready plumber.ReadyFunc) error {
				ready()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(25 * time.Second):
					return errors.New("runner failed")
				}
			}),
		).With(plumber.Signaler(signaler)),
		plumber.TTL(50*time.Millisecond),
		plumber.Closing(signaler),
	)
	// Context should be canceled since:
	// - first runner immediately closes
	// - close sequence is initiated
	// - closing is immediately done
	// - start context is canceled
	fmt.Println(err)
	assert.Assert(t, errors.Is(err, context.Canceled))
}

func TestPipelineRunnerContextCanceled(t *testing.T) {
	ctx := context.Background()

	signaler := plumber.NewErrorSignaler()

	err := plumber.Start(ctx,
		plumber.Pipeline(
			reportingBlockingRunner("runner 1"),
			plumber.Runner(func(ctx context.Context, ready plumber.ReadyFunc) error {
				ready()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
					return errors.New("runner failed")
				}
			}),
		).With(plumber.Signaler(signaler)),
		plumber.TTL(10*time.Millisecond),
		plumber.CloseTimeout(10*time.Millisecond),
		plumber.Closing(signaler),
	)
	assert.ErrorContains(t, err, "context canceled")
}

func TestParallelPipeline(t *testing.T) {
	ctx := context.Background()

	runner := plumber.Runner(func(ctx context.Context, ready plumber.ReadyFunc) error {
		ready()
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	start := time.Now()
	err := plumber.Start(ctx,
		plumber.Parallel(
			runner,
			runner,
			runner,
			runner,
			runner,
			runner,
		),
		plumber.TTL(1*time.Second),
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%v\n", time.Since(start))
	assert.Assert(t, time.Since(start) <= 15*time.Millisecond)
}
