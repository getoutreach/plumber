package plumber_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getoutreach/plumber"
	"gotest.tools/v3/assert"
)

func reportingRunner(name string, fce ...func()) plumber.Runner {
	return plumber.GracefulRunner(func(ctx context.Context) error {
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
func reportingBlockingRunner(name string, fce ...func()) plumber.Runner {
	return plumber.GracefulRunner(func(ctx context.Context) error {
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

func erroringRunner(name string) plumber.Runner {
	return plumber.NewRunner(func(ctx context.Context) error {
		return errors.New("runner " + name + " failed")
	})
}

// nolint: unused //Why: not yet
func erroringCloser(name string) plumber.Runner {
	return plumber.Closer(func(ctx context.Context) error {
		return errors.New("runner " + name + " failed")
	})
}

func TestCloseTimeout(t *testing.T) {
	ctx := context.Background()

	err := plumber.Start(ctx,
		plumber.Pipeline(
			reportingBlockingRunner("runner 1"),
			reportingBlockingRunner("runner 2"),
			reportingBlockingRunner("runner 3"),
		),
		plumber.TTL(10*time.Millisecond),
		plumber.CloseTimeout(100*time.Millisecond),
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		fmt.Printf("err nil or: %v", err)
	}
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
		plumber.CloseTimeout(100*time.Millisecond),
		func(ctx context.Context, o *plumber.Options) context.Context {
			o.Closer(func(ctx context.Context) error {
				return errors.New("Closer error")
			})
			return ctx
		},
	)
	assert.ErrorContains(t, err, "runner runner 1 failed")
	assert.ErrorContains(t, err, "Closer error")
}

func TestPipelineSignalerClosing(t *testing.T) {
	ctx := context.Background()

	err := plumber.Start(ctx,
		plumber.Pipeline(
			reportingRunner("runner 1", func() {
				// Let other job to start as well
				time.Sleep(10 * time.Millisecond)
			}),
			plumber.NewRunner(func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(10 * time.Second):
					return errors.New("runner failed")
				}
			}),
		),
		plumber.TTL(50*time.Millisecond),
	)
	// Context should be canceled since:
	// - first runner immediately closes
	// - close sequence is initiated
	// - closing is immediately done
	// - start context is canceled
	fmt.Println(err)
	assert.Assert(t, errors.Is(err, context.Canceled))
}

func TestPipelineCloseOnError(t *testing.T) {
	ctx := context.Background()

	closed := make(chan time.Time, 3)
	started := time.Now()

	reportingClose := func(ctx context.Context) error {
		closed <- time.Now()
		return nil
	}

	err := plumber.Start(ctx,
		plumber.Pipeline(
			plumber.NewRunner(func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(25 * time.Second):
					return errors.New("runner 1 timeout")
				}
			}, plumber.WithClose(reportingClose)),
			plumber.Pipeline(
				plumber.NewRunner(func(ctx context.Context) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(25 * time.Second):
						return errors.New("runner 3 timeout")
					}
				}, plumber.WithClose(reportingClose)),
				plumber.NewRunner(func(ctx context.Context) error {
					return errors.New("runner 2 failed")
				}, plumber.WithClose(reportingClose)),
			),
		),
		plumber.TTL(3*time.Second),
	)
	close(closed)
	// Context should be canceled since:
	// - first runner immediately closes
	// - close sequence is initiated
	// - closing is immediately done
	// - start context is canceled
	fmt.Println(err)
	n := 0
	for tm := range closed {
		n++
		//time.Now().
		diff := tm.Sub(started)
		fmt.Println(diff)
		assert.Assert(t, diff < 100*time.Millisecond)
	}
	assert.Equal(t, n, 3)
}

func TestPipelineRunnerContextCanceled(t *testing.T) {
	ctx := context.Background()

	err := plumber.Start(ctx,
		plumber.Pipeline(
			reportingBlockingRunner("runner 1"),
			plumber.ReadyRunner(func(ctx context.Context, ready plumber.ReadyFunc) error {
				ready()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
					return errors.New("runner failed")
				}
			}),
		),
		plumber.TTL(10*time.Millisecond),
		plumber.CloseTimeout(10*time.Millisecond),
	)
	assert.ErrorContains(t, err, "context deadline exceeded")
}

func TestParallelPipeline(t *testing.T) {
	ctx := context.Background()

	runner := plumber.ReadyRunner(func(ctx context.Context, ready plumber.ReadyFunc) error {
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

func TestPipelineDetachedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var (
		logger           = log.New(log.Writer(), "test: ", log.LstdFlags|log.Lmicroseconds)
		brutallyCanceled int32
	)

	runner := plumber.PipelineRunner(
		plumber.Pipeline(
			plumber.Looper(func(ctx context.Context, loop *plumber.Loop) error {
				loop.Ready()
				for {
					select {
					case closed := <-loop.Closing():
						logger.Print("closing looper")
						closed.Success()
						return nil
					case <-ctx.Done():
						atomic.StoreInt32(&brutallyCanceled, 1)
						logger.Print("looper context done")
						return fmt.Errorf("context brutally canceled: %w", ctx.Err())
					default:
						logger.Print("looper working")
						time.Sleep(30 * time.Millisecond)
					}
				}
			}),
		),
		plumber.TTL(1*time.Second),
		plumber.CloseTimeout(2*time.Second),
		plumber.DetachContext(),
	)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := runner.Run(ctx)
		assert.NilError(t, err)
	}()

	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		logger.Print("canceling parent context")
		cancel()
	}()

	wg.Wait()

	assert.Assert(t, brutallyCanceled == 0)
}

func TestPipelineRunnerClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var (
		logger           = log.New(log.Writer(), "test: ", log.LstdFlags|log.Lmicroseconds)
		brutallyCanceled int32
	)

	runner := plumber.PipelineRunner(
		plumber.Pipeline(
			plumber.Looper(func(ctx context.Context, loop *plumber.Loop) error {
				loop.Ready()
				for {
					select {
					case closed := <-loop.Closing():
						time.Sleep(100 * time.Millisecond)
						logger.Print("closing looper")
						closed.Success()
						return nil
					case <-ctx.Done():
						atomic.StoreInt32(&brutallyCanceled, 1)
						logger.Print("looper context done")
						return fmt.Errorf("context brutally canceled: %w", ctx.Err())
					default:
						logger.Print("looper working")
						time.Sleep(30 * time.Millisecond)
					}
				}
			}),
		),
	)
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		err := runner.Run(ctx)
		// If run managed to return we have nil, if cancel closes the context we get context.Canceled
		// For gracefull shutdown use CloseTimeout
		assert.NilError(t, plumber.UnlessCanceled(err))
	}()

	go func() {
		defer wg.Done()
		time.Sleep(500 * time.Millisecond)
		logger.Print("closing runner twice")
		err := runner.Close(context.Background())
		assert.NilError(t, err)
	}()

	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		logger.Print("closing runner")
		err := runner.Close(context.Background())
		assert.NilError(t, err)
	}()

	wg.Wait()

	assert.Assert(t, brutallyCanceled == 0)
}

func TestPipelineSignalCloser(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signal := plumber.NewSignal()

	var (
		logger             = log.New(log.Writer(), "test: ", log.LstdFlags|log.Lmicroseconds)
		brutallyCanceled   int32
		gracefullyCanceled int32
	)

	runner := plumber.PipelineRunner(
		plumber.Pipeline(
			plumber.Looper(func(ctx context.Context, loop *plumber.Loop) error {
				loop.Ready()
				for {
					select {
					case closed := <-loop.Closing():
						atomic.StoreInt32(&gracefullyCanceled, 1)
						logger.Print("closing looper")
						closed.Success()
						return nil
					case <-ctx.Done():
						atomic.StoreInt32(&brutallyCanceled, 1)
						logger.Print("looper context done")
						return fmt.Errorf("context brutally canceled: %w", ctx.Err())
					default:
						logger.Print("looper working")
						time.Sleep(30 * time.Millisecond)
					}
				}
			}),
		),
		plumber.CloseTimeout(2*time.Second),
		plumber.SignalChannelCloser(signal),
	)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := runner.Run(ctx)
		assert.NilError(t, err)
	}()

	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		signal.Notify()
	}()

	wg.Wait()

	assert.Assert(t, brutallyCanceled == 0)
	assert.Assert(t, gracefullyCanceled == 1)
}

// The test is intended not to hang
func TestPipelineCloseBeforeRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	runner := plumber.PipelineRunner(
		plumber.Pipeline(
			plumber.Looper(func(ctx context.Context, loop *plumber.Loop) error {
				loop.Ready()
				for {
					select {
					case closed := <-loop.Closing():
						closed.Success()
						return nil
					case <-ctx.Done():
						return ctx.Err()
					default:
						time.Sleep(100 * time.Millisecond)
					}
				}
			}),
		),
	)
	var wg sync.WaitGroup
	wg.Add(1)

	runner.Close(context.Background())

	go func() {
		defer wg.Done()
		err := plumber.UnlessCanceled(runner.Run(ctx))
		assert.NilError(t, err)
	}()

	wg.Wait()
}
