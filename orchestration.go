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

// ReadyFunc is a func used to report that worker is ready
type ReadyFunc func()

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
	// Run executes runners workload. Pipelines are starting Run method in separated goroutine.
	// Runner must report its readiness using given callback
	Run(ctx context.Context, ready ReadyFunc) error

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
	run   func(ctx context.Context, ready ReadyFunc) error
	close func(ctx context.Context) error
}

// GracefulRunner provides easy way to construct simple graceful runner providing run and close functions
func GracefulRunner(
	runFunc func(ctx context.Context, ready ReadyFunc) error,
	closeFunc func(ctx context.Context) error,
) RunnerCloser {
	return &gracefulRunner{
		run:   runFunc,
		close: closeFunc,
	}
}

// Run executes internal run callback
// It partially implement RunnerCloser interface
func (r *gracefulRunner) Run(ctx context.Context, ready ReadyFunc) error {
	return r.run(ctx, ready)
}

// Close executes internal close callback
// It partially implement RunnerCloser interface
func (r *gracefulRunner) Close(ctx context.Context) error {
	return r.close(ctx)
}

// Loop is a looper controlling struct
type Loop struct {
	closeCh chan DoneFunc
	ready   ReadyFunc
}

// Run invokes given callback in the detached goroutine. The error returned from the callback will be returned when Close method is invoked
func (l *Loop) Run(run func(ready ReadyFunc) error) error {
	return run(l.ready)
}

// Ready reports runner's readiness
func (l *Loop) Ready() {
	l.ready()
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
	var (
		once sync.Once
		l    = &Loop{
			closeCh: make(chan DoneFunc, 1),
		}
	)
	return &gracefulRunner{
		run: func(ctx context.Context, ready ReadyFunc) error {
			var err error
			once.Do(func() {
				l.ready = ready
				defer close(l.closeCh)
				err = run(ctx, l)
			})
			return err
		},
		close: func(ctx context.Context) error {
			var (
				errCh             = make(chan error, 1)
				canceled DoneFunc = func(err error) {
					errCh <- err
					close(errCh)
				}
			)
			l.closeCh <- canceled
			// if hasn't been started, lets close it
			once.Do(func() {
				close(errCh)
				close(l.closeCh)
			})
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
func Runner(run func(ctx context.Context, ready ReadyFunc) error) RunnerCloser {
	return &gracefulRunner{
		run: run,
		close: func(ctx context.Context) error {
			return nil
		},
	}
}

// SimpleRunner creates a RunnerCloser based on supplied run function with noop Clone method, ready state is reported automatically
func SimpleRunner(run func(ctx context.Context) error) RunnerCloser {
	return &gracefulRunner{
		run: func(ctx context.Context, ready ReadyFunc) error {
			ready()
			return run(ctx)
		},
		close: func(ctx context.Context) error {
			return nil
		},
	}
}

// Closer creates a RunnerCloser based on supplied close function with noop Run method
func Closer(closeFunc CallbackFunc) RunnerCloser {
	return &gracefulRunner{
		close: closeFunc,
		run: func(ctx context.Context, ready ReadyFunc) error {
			ready()
			<-ctx.Done()
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
func (r *ParallelPipeline) Run(ctx context.Context, ready ReadyFunc) error {
	var (
		readyCh = make(chan struct{}, len(r.runners))
		errs    = make(ErrorCh, len(r.runners))
	)
	go func() {
		var counter = 0
		var isReady bool
		for {
			select {
			case <-readyCh:
				counter++
			case <-ctx.Done():
				break
			}
			if counter == len(r.runners) {
				isReady = true
				break
			}
		}
		if isReady {
			ready()
		}
		r.wg.Wait()
		close(errs)
	}()

	r.wg.Add(len(r.runners))
	for _, runner := range r.runners {
		go func(runner RunnerCloser) {
			defer r.wg.Done()
			if err := runner.Run(ctx, func() {
				// Signal that runner is ready
				readyCh <- struct{}{}
			}); err != nil {
				if r.options.ErrorSignaler != nil && !r.closing.Load() {
					r.options.ErrorSignaler(err)
				}
				errs <- err
			}
		}(runner)
	}

	return errors.Join(errs.Errors()...)
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
func (r *SerialPipeline) Run(ctx context.Context, ready ReadyFunc) error {
	var (
		wg      sync.WaitGroup
		errs    = make(ErrorCh, len(r.runners))
		readyCh = make(chan struct{}, 1)
		closeCh = make(chan struct{})
	)
	wg.Add(len(r.runners))

	drain := func(index int) {
		for i := index; i < len(r.runners); i++ {
			wg.Done()
		}
	}

	// started go routine
	go func() {
		var index = 0
		for {
			select {
			case <-closeCh:
				drain(index)
				return
			case <-readyCh:
				// when all runners are running we cal report that pipeline is ready
				if index == len(r.runners) {
					ready()
					return
				}
				// when we are closing we need to mark remaining workers as finished
				if r.closing.Load() {
					drain(index)
					return
				}
				runner := r.runners[index]
				index++
				// runner go routine
				go func() {
					var once sync.Once
					defer wg.Done()
					err := runner.Run(ctx, func() {
						// worker is ready we can start with next one
						once.Do(func() {
							readyCh <- struct{}{}
						})
					})
					if r.options.ErrorSignaler != nil && !r.closing.Load() {
						r.options.ErrorSignaler(err)
					}
					if err != nil {
						r.closing.Store(true)
						close(closeCh)
					}
					errs <- err
				}()
			case <-ctx.Done():
				return
			}
		}
	}()
	// Lets start first worker
	readyCh <- struct{}{}

	wg.Wait()
	close(errs)

	return errors.Join(errs.Errors()...)
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
		options = &Options{}
	)
	startCtx, startCancel := context.WithCancelCause(ctx)
	defer startCancel(nil)

	closers := newCloserContext(startCtx)
	defer closers.cancel()

	var (
		errorCh       = make(ErrorCh, 3)
		closeCh       = make(chan struct{}, 1)
		once          sync.Once
		chanWriters   sync.WaitGroup
		closeChannels = func() {
			closers.cancel()
			close(closeCh)
			chanWriters.Wait()
			// We can really terminate since all channel writers are done
			close(errorCh)
		}
		terminate = func(ctx context.Context, initiateClose bool) {
			once.Do(func() {
				if !initiateClose {
					closeChannels()
					return
				}
				closeCtx, closeCancel := options.closeContext(ctx)
				defer closeCancel()
				// go routine handling close async so it can be canceled
				go func() {
					err := runner.Close(closeCtx)
					startCancel(closeCtx.Err())
					errorCh <- err
					closeChannels()
				}()
				select {
				// Wait for close to finish
				case <-closeCh:
					break
				// Wait for close context to be canceled
				case <-closeCtx.Done():
					startCancel(closeCtx.Err())
					if err := closeCtx.Err(); err != nil {
						errorCh <- err
					}
					break
				}
			})
		}
	)
	chanWriters.Add(2)

	options.close = func() {
		go func() {
			terminate(ctx, true)
		}()
	}
	options.Apply(opts...)

	closers.start(errorCh, &chanWriters, options.closers...)

	// runner go routine
	go func() {
		defer chanWriters.Done()
		err := runner.Run(startCtx, func() {
			// We might expose ready callback later via pipeline options
		})
		if err != nil {
			// runner sequence had a problems calling close
			errorCh <- err
		}
		go terminate(ctx, false)
	}()

	return errors.Join(errorCh.Wait(ctx)...)
}

// newCloserContext return instance of *closerContext
func newCloserContext(startCtx context.Context) *closerContext {
	var erg, closerCtx = errgroup.WithContext(startCtx)
	closerCtx, closerCancel := context.WithCancel(closerCtx)
	return &closerContext{
		erg:    erg,
		cancel: closerCancel,
		ctx:    closerCtx,
	}
}

// closerContext a helper struct managing pipeline closers
type closerContext struct {
	erg    *errgroup.Group
	ctx    context.Context
	cancel func()
}

// start starts closers functions and captures an error
func (c *closerContext) start(errorCh chan error, chanWriters *sync.WaitGroup, closers ...func(context.Context) error) {
	for _, closer := range closers {
		closer := closer
		c.erg.Go(func() error {
			return closer(c.ctx)
		})
	}

	// closers go routine
	go func() {
		defer chanWriters.Done()
		err := c.erg.Wait()
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			errorCh <- err
		}
	}()
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
