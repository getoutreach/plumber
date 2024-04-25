// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains task orchestration code
package plumber

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// DoneFunc is a func used to report that worker has finished
type DoneFunc func(error)

// Done reports back runner status using given error
func (f DoneFunc) Done(do func() error) {
	f(do())
}

// Success reports back runner status as success. It is preferred then running Done(nil) to increase code readability.
func (f DoneFunc) Success() {
	f(nil)
}

// RunnerCloser describes a runnable task
type RunnerCloser interface {
	// Run is responsible for initializing and starting a task.
	// When method returns orchestrator might decided to start another RunnerCloser.
	// Runner must spawn goroutine when task should be running after Run method finishes.
	Run(ctx context.Context, done DoneFunc) error

	// Close method triggers graceful shutdown on the task. It should block till task is properly closed.
	// When Close timeout is exceeded then given context is canceled.
	Close(ctx context.Context) error
}

// ErrorCh is a channel of errors
type ErrorCh chan error

// Errors returns errors accumulated in the channel. This function blockes until the channel is closed
func (ec ErrorCh) Errors() []error {
	errs := []error{}
	for err := range ec {
		errs = append(errs, err)
	}
	return errs
}

// Wait waits till channel is not close or till given context is ended and then returns all accumulated errors.
func (ec ErrorCh) Wait(ctx context.Context) []error {
	var (
		errs = []error{}
		wg   sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-ec:
				if !ok {
					return
				}
				errs = append(errs, err)
			}
		}
	}()
	wg.Wait()
	return errs
}

// CallbackFunc a callback function type for graceful runner
type CallbackFunc func(context.Context) error

// gracefulRunner struct
type gracefulRunner struct {
	run   func(ctx context.Context, done DoneFunc) error
	close func(ctx context.Context) error
}

// GracefulRunner provides easy way to construct simple graceful runner providing run and close functions
func GracefulRunner(
	runFunc func(ctx context.Context, done DoneFunc) error,
	closeFunc func(ctx context.Context) error,
) RunnerCloser {
	return &gracefulRunner{
		run:   runFunc,
		close: closeFunc,
	}
}

// Run executes internal run callback
// It partially implement RunnerCloser interface
func (r *gracefulRunner) Run(ctx context.Context, done DoneFunc) error {
	return r.run(ctx, done)
}

// Close executes internal close callback
// It partially implement RunnerCloser interface
func (r *gracefulRunner) Close(ctx context.Context) error {
	return r.close(ctx)
}

// Loop is a looper controlling struct
type Loop struct {
	closeCh chan DoneFunc
	done    DoneFunc
}

// Run invokes given callback in the detached goroutine. The error returned from the callback will be returned when Close method is invoked
func (l *Loop) Run(run func(done DoneFunc)) error {
	go func() {
		run(l.done)
	}()
	return nil
}

// Closing returns a channel that's closed when cancellation is requested
func (l *Loop) Closing() <-chan DoneFunc {
	return l.closeCh
}

// Looper creates a graceful RunnerCloser that runs in the detached go routine in indefinite loop.
// Provided Loop struct allows to detect a cancellation and graceful termination
// Example:
//
//	plumber.Looper(func(ctx context.Context, l *plumber.Loop) error {
//	    return l.Run(func(done plumber.DoneFunc) {
//	        tick := time.Tick(500 * time.Millisecond)
//	        fmt.Println("Looper starting up")
//	        done.Done(func() error {
//	            for {
//	                select {
//	                case <-tick:
//	                    fmt.Println("Looper work")
//	                case done := <-l.Closing():
//	                    fmt.Println("Looper requested to shutdown")
//	                    done.Success()
//	                    fmt.Println("Looper finished")
//	                    return nil
//	                case <-ctx.Done():
//	                    fmt.Println("Looper canceled")
//	                    // Cancel / Timeout
//	                    return ctx.Err()
//	                }
//	            }
//	        })
//	    })
//	})
func Looper(run func(ctx context.Context, loop *Loop) error) RunnerCloser {
	l := &Loop{
		closeCh: make(chan DoneFunc),
	}
	return &gracefulRunner{
		run: func(ctx context.Context, done DoneFunc) error {
			l.done = done
			return run(ctx, l)
		},
		close: func(ctx context.Context) error {
			var (
				errCh             = make(chan error, 1)
				canceled DoneFunc = func(err error) {
					errCh <- err
				}
			)
			l.closeCh <- canceled
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-errCh:
				return err
			}
		},
	}
}

// Runner creates a RunnerCloser based on supplied run function with noop Clone method
func Runner(run func(ctx context.Context, done DoneFunc) error) RunnerCloser {
	return &gracefulRunner{
		run: run,
		close: func(ctx context.Context) error {
			return nil
		},
	}
}

// Closer creates a RunnerCloser based on supplied close function with noop Run method
func Closer(closeFunc CallbackFunc) RunnerCloser {
	return &gracefulRunner{
		close: closeFunc,
		run: func(ctx context.Context, done DoneFunc) error {
			defer done.Success()
			return nil
		},
	}
}

// ParallelPipeline is a parallel runner closer orchestrator
// The runners are started and closed in concurrent fashion.
// The Run or Close are invoked independently
type ParallelPipeline struct {
	runners []RunnerCloser
	wg      sync.WaitGroup
	options *PipelineOptions
	closing atomic.Bool
}

// Parallel creates a concurrent RunnerCloser executor.
// When started it will execute runners Run and Close methods in parallel.
// Run and Close will block till all runner's corresponding methods are returned.
func Parallel(runners ...RunnerCloser) *ParallelPipeline {
	return &ParallelPipeline{
		runners: runners,
		options: &PipelineOptions{},
	}
}

// Run executes Run method on internal runners in parallel.
// It partially implement RunnerCloser interface.
// The it returns when all runner's Run methods are returned.
func (r *ParallelPipeline) Run(ctx context.Context, done DoneFunc) error {
	r.wg.Add(len(r.runners))
	errs := make(ErrorCh, len(r.runners))

	go func() {
		r.wg.Wait()
		close(errs)
		er := errors.Join(errs.Errors()...)
		done(er)
	}()

	for _, runner := range r.runners {
		go func(runner RunnerCloser) {
			var once = sync.Once{}
			if err := runner.Run(ctx, func(err error) {
				once.Do(func() {
					if r.options.ErrorSignaler != nil && !r.closing.Load() {
						r.options.ErrorSignaler(err)
					}
					if err != nil {
						errs <- err
					}
					r.wg.Done()
				})
			}); err != nil {
				errs <- err
			}
		}(runner)
	}

	return nil
}

// Close executes Close method on internal runners in in parallel.
// It partially implement RunnerCloser interface.
// The it returns when all runner's Close methods are returned.
func (r *ParallelPipeline) Close(ctx context.Context) error {
	r.closing.Store(true)
	var (
		closeErrors = make(ErrorCh, len(r.runners))
		wg          sync.WaitGroup
	)
	wg.Add(len(r.runners))
	for i := len(r.runners) - 1; i >= 0; i-- {
		var runner = r.runners[i]
		go func() {
			defer wg.Done()
			if err := runner.Close(ctx); err != nil {
				closeErrors <- err
			}
		}()
	}
	wg.Wait()
	close(closeErrors)
	return errors.Join(closeErrors.Errors()...)
}

// With applies the pipeline options
func (r *ParallelPipeline) With(oo ...PipelineOption) *ParallelPipeline {
	r.options.Apply(oo...)
	return r
}

// SerialPipeline is a serial runner closer orchestrator
// The runners are started and closed in serial fashion.
// The Run or Close methods needs to return and only then next runner is evaluated
type SerialPipeline struct {
	runners []RunnerCloser
	options *PipelineOptions
	closing atomic.Bool
}

// Pipeline creates a serial RunnerCloser executor.
// When started it will execute Run method on given runners one by one with given order.
// When closed it will execute Close method on given runners in revered order to achieve graceful shutdown sequence
func Pipeline(runners ...RunnerCloser) *SerialPipeline {
	return &SerialPipeline{
		runners: runners,
		options: &PipelineOptions{},
	}
}

// Run executes Run method on internal runners one by one with given order.
// It partially implement RunnerCloser interface
func (r *SerialPipeline) Run(ctx context.Context, done DoneFunc) error {
	var (
		wg sync.WaitGroup
	)
	wg.Add(len(r.runners))

	errs := make(ErrorCh, len(r.runners))

	// Wait first since we might get blocked
	go func() {
		wg.Wait()
		close(errs)
		er := errors.Join(errs.Errors()...)
		done(er)
	}()

	for _, runner := range r.runners {
		err := func(runner RunnerCloser) error {
			var once = sync.Once{}
			if err := runner.Run(ctx, func(err error) {
				once.Do(func() {
					if r.options.ErrorSignaler != nil && !r.closing.Load() {
						r.options.ErrorSignaler(err)
					}
					if err != nil {
						errs <- err
					}
					wg.Done()
				})
			}); err != nil {
				return err
			}
			return nil
		}(runner)
		if err != nil {
			return err
		}
	}

	return nil
}

// Close executes Close method on internal runners in revered order to achieve graceful shutdown sequence
// It partially implement RunnerCloser interface
func (r *SerialPipeline) Close(ctx context.Context) error {
	r.closing.Store(true)
	var closeErrors []error
	for i := len(r.runners) - 1; i >= 0; i-- {
		var runner = r.runners[i]
		if err := runner.Close(ctx); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	return errors.Join(closeErrors...)
}

// With applies the pipeline options
func (r *SerialPipeline) With(oo ...PipelineOption) *SerialPipeline {
	r.options.Apply(oo...)
	return r
}

// Start will execute given runner with optional configuration
func Start(ctx context.Context, runner RunnerCloser, opts ...Option) error {
	var (
		options      = &Options{}
		closerCancel func()
	)
	startCtx, startCancel := context.WithCancelCause(ctx)
	defer startCancel(nil)

	var erg, closerCtx = errgroup.WithContext(startCtx)
	closerCtx, closerCancel = context.WithCancel(closerCtx)
	defer closerCancel()

	var (
		errorCh     = make(ErrorCh, 3)
		closeCh     = make(chan struct{}, 1)
		once        sync.Once
		closerGroup sync.WaitGroup
		closeRunner = func(ctx context.Context, runner RunnerCloser) {
			once.Do(func() {
				defer closerGroup.Done()
				closeCtx, closeCancel := context.WithCancel(ctx)
				defer closeCancel()
				if options.CloseTimeout > 0 {
					closeCtx, closeCancel = context.WithTimeout(closeCtx, options.CloseTimeout)
					defer closeCancel()
				}
				go func() {
					err := runner.Close(closeCtx)
					errorCh <- err
					closerCancel()
					close(closeCh)
					closerGroup.Wait()
					// We can really terminate since all channel writers are done
					close(errorCh)
				}()
				defer func() {
					startCancel(closeCtx.Err())
				}()
				select {
				// Wait for close to finish
				case <-closeCh:
					break
				// Wait for close context to be canceled
				case <-closeCtx.Done():
					if err := closeCtx.Err(); err != nil {
						errorCh <- err
					}
					break
				}
				// Close the channel so we can terminate
			})
		}
	)
	closerGroup.Add(3)

	options.close = func() {
		go func() {
			closeRunner(ctx, runner)
		}()
	}
	options.Apply(opts...)

	for _, closer := range options.closers {
		closer := closer
		erg.Go(func() error {
			return closer(closerCtx)
		})
	}

	go func() {
		err := erg.Wait()
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			errorCh <- err
		}
		closerGroup.Done()
	}()

	go func() {
		var (
			once      sync.Once
			callClose = func(err error) {
				if err != nil {
					errorCh <- err
				}
				closerGroup.Done()
				once.Do(func() {
					closeRunner(ctx, runner)
				})
			}
		)

		err := runner.Run(startCtx, func(err error) {
			// We need to wait for runners to finish before closing errorCh
			// runner finished calling close
			callClose(err)
		})
		if err != nil {
			closerGroup.Add(1)
			// runner sequence had a problems calling close
			callClose(err)
		}
	}()

	return errors.Join(errorCh.Wait(ctx)...)
}

// PipelineOptions holds a pipeline options
type PipelineOptions struct {
	ErrorSignaler func(error)
}

// PipelineOption is a option patter struct for PipelineOptions
type PipelineOption func(*PipelineOptions)

// Apply given PipelineOption
func (o *PipelineOptions) Apply(oo ...PipelineOption) *PipelineOptions {
	for _, op := range oo {
		op(o)
	}
	return o
}

// Signaler is a pipeline options setting given signaler.
// The signaler is used to inform signaler listeners that certain task has finished with that pipeline.
// This might get handy to broadcast an error and close the whole pipeline when it occur.
func Signaler(s *ErrorSignaler) func(*PipelineOptions) {
	return func(o *PipelineOptions) {
		o.ErrorSignaler = s.Signal
	}
}
