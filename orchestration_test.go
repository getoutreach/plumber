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

func reportingRunner(name string) plumber.RunnerCloser {
	return plumber.GracefulRunner(func(ctx context.Context, done plumber.DoneFunc) error {
		defer done.Success()
		fmt.Println("runner", name, "started")
		return nil
	}, func(ctx context.Context) error {
		fmt.Println("runner", name, "closed")
		return nil
	})
}

func erroringCloser(name string) plumber.RunnerCloser {
	return plumber.Closer(func(ctx context.Context) error {
		return errors.New("runner " + name + " failed")
	})
}

func ExamplePipeline() {
	ctx := context.Background()

	err := plumber.Start(ctx,
		plumber.Pipeline(
			reportingRunner("runner 1"),
			reportingRunner("runner 2"),
			reportingRunner("runner 3"),
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
			erroringCloser("runner 1"),
			erroringCloser("runner 2"),
			erroringCloser("runner 3"),
		),
		plumber.TTL(10*time.Millisecond),
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
			reportingRunner("runner 1"),
			plumber.Runner(func(ctx context.Context, done plumber.DoneFunc) error {
				go func() {
					select {
					case <-ctx.Done():
						done(ctx.Err())
						break
					case <-time.After(10 * time.Millisecond):
						done(errors.New("runner failed"))
						break
					}
				}()
				return nil
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
	assert.Assert(t, errors.Is(err, context.Canceled))
}

func TestPipelineRunnerContextCanceled(t *testing.T) {
	ctx := context.Background()

	signaler := plumber.NewErrorSignaler()

	err := plumber.Start(ctx,
		plumber.Pipeline(
			reportingRunner("runner 1"),
			plumber.Runner(func(ctx context.Context, done plumber.DoneFunc) error {
				go func() {
					select {
					case <-ctx.Done():
						done(ctx.Err())
						break
					case <-time.After(2 * time.Second):
						done(errors.New("runner failed"))
						break
					}
				}()
				return nil
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

	runner := plumber.Runner(func(ctx context.Context, done plumber.DoneFunc) error {
		time.Sleep(10 * time.Millisecond)
		go func() {
			done.Success()
		}()
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
