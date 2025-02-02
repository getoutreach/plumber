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

// ReadyRunner creates a Runner based on supplied run function with callback to signal ready state
func ReadyRunner(run func(ctx context.Context, ready ReadyFunc) error) Runner {
	signal := NewSignal()
	return NewRunner(func(ctx context.Context) error {
		return run(ctx, func() {
			signal.Notify()
		})
	}, WithReady(signal))
}

// Closer creates a runner implementing Closer interface based on supplied close function with noop Run method
func Closer(closeFunc CallbackFunc) Runner {
	return NewRunner(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}, WithClose(closeFunc))
}

// ParallelPipeline is a parallel runner closer orchestrator
// The runners are started and closed in concurrent fashion.
// The Run or Close are invoked independently
type ParallelPipeline struct {
	runners []Runner
	wg      sync.WaitGroup
	options *PipelineOptions
	closing atomic.Bool

	closed    chan struct{}
	closeOnce sync.Once
	signal    *Signal
	errSignal *Signal
}

// Parallel creates a concurrent Runner executor.
// When started it will execute runners Run and Close methods in parallel.
// Run and Close will block till all runner's corresponding methods are returned.
func Parallel(runners ...Runner) *ParallelPipeline {
	return &ParallelPipeline{
		runners:   runners,
		options:   &PipelineOptions{},
		signal:    NewSignal(),
		errSignal: NewSignal(),
		closed:    make(chan struct{}),
	}
}

func (r *ParallelPipeline) Errored() <-chan struct{} {
	return r.errSignal.C()
}

func (r *ParallelPipeline) Ready() (<-chan struct{}, error) {
	return r.signal.C(), nil
}

// Run executes Run method on internal runners in parallel.
// It partially implement Runner interface.
// The it returns when all runner's Run methods are returned.
func (r *ParallelPipeline) Run(ctx context.Context) error {
	var (
		readyCh = make(chan struct{}, len(r.runners))
		errs    = make(ErrorCh, len(r.runners))
	)
	r.wg.Add(len(r.runners))

	go func() {
		var counter = 0
		for {
			select {
			case <-readyCh:
				counter++
			case <-r.closed:
				break
			case <-ctx.Done():
				break
			}
			if counter == len(r.runners) {
				r.signal.Notify()
				break
			}
		}
		r.wg.Wait()
		close(errs)
	}()

	if !r.options.KeepRunningOnError {
		closeOnError(ctx, r.errSignal, r)
	}

	for _, runner := range r.runners {
		go func(runner Runner) {
			defer r.wg.Done()

			// Wait for the runner to be ready
			go func() {
				ready, err := RunnerReady(runner)
				if err != nil {
					errs <- err
					r.Close(ctx)
					return
				}
				select {
				case <-ready:
					readyCh <- struct{}{}
				case <-ctx.Done():
				}
			}()

			go forwardErrorSignal(ctx, runner, r.closed, r.errSignal)

			err := runner.Run(ctx)
			if err != nil && !r.closing.Load() {
				r.errSignal.Notify()
			}
			errs <- err
		}(runner)
	}

	return errors.Join(errs.Errors()...)
}

// Close executes Close method on internal runners in in parallel.
// It partially implement Closer interface.
// The it returns when all runner's Close methods are returned.
func (r *ParallelPipeline) Close(ctx context.Context) error {
	r.closeOnce.Do(func() {
		close(r.closed)
	})
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
			if err := RunnerClose(ctx, runner); err != nil {
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
	runners   []Runner
	options   *PipelineOptions
	closing   atomic.Bool
	closed    chan struct{}
	closeOnce sync.Once
	signal    *Signal
	errSignal *Signal
}

// Pipeline creates a serial Runner executor.
// When started it will execute Run method on given runners one by one with given order.
// When closed it will execute Close method on given runners in revered order to achieve graceful shutdown sequence
func Pipeline(runners ...Runner) *SerialPipeline {
	return &SerialPipeline{
		runners:   runners,
		options:   &PipelineOptions{},
		closed:    make(chan struct{}),
		signal:    NewSignal(),
		errSignal: NewSignal(),
	}
}

func (r *SerialPipeline) Errored() <-chan struct{} {
	return r.errSignal.C()
}

func (r *SerialPipeline) Ready() (<-chan struct{}, error) {
	return r.signal.C(), nil
}

// Run executes Run method on internal runners one by one with given order.
func (r *SerialPipeline) Run(ctx context.Context) error {
	var (
		wg      sync.WaitGroup
		errs    = make(ErrorCh, len(r.runners))
		readyCh = make(chan struct{}, 1)
		errored atomic.Bool
	)

	if !r.options.KeepRunningOnError {
		closeOnError(ctx, r.errSignal, r)
	}

	// orchestration go routine
	go func() {
		defer wg.Done()
		var index = 0
		for {
			select {
			case _, ok := <-readyCh:
				// We are closed
				if !ok {
					return
				}

				// when all runners are running we cal report that pipeline is ready
				if index == len(r.runners) {
					r.signal.Notify()
					return
				}

				// We need to check those again since select does not guarantee the priority
				select {
				case <-r.closed:
				case <-ctx.Done():
				default:
					runner := r.runners[index]
					index++

					wg.Add(1)
					// runner go routine
					go func() {
						defer wg.Done()
						if errored.Load() && r.closing.Load() {
							return
						}

						wg.Add(1)
						// Wait for the runner to become ready
						// ready checking goroutine
						go func() {
							defer wg.Done()

							ready, err := RunnerReady(runner)
							if err != nil {
								errs <- err
								r.Close(ctx)
								return
							}

							select {
							case <-r.closed:
							case <-ctx.Done():
							case <-ready:
								readyCh <- struct{}{}
							}
						}()

						go forwardErrorSignal(ctx, runner, r.closed, r.errSignal)

						err := runner.Run(ctx)
						if err != nil && !r.closing.Load() {
							r.errSignal.Notify()
						}
						if err != nil {
							errored.Store(true)
							errs <- err
						}
					}()
				}
			case <-r.closed:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	// Lets start first worker
	wg.Add(1)

	readyCh <- struct{}{}
	wg.Wait()

	close(errs)
	close(readyCh)

	return errors.Join(errs.Errors()...)
}

// Close executes Close method on internal runners in revered order to achieve graceful shutdown sequence
// It implements Closer interface
func (r *SerialPipeline) Close(ctx context.Context) error {
	var closeErrors []error
	r.closeOnce.Do(func() {
		close(r.closed)
		for i := len(r.runners) - 1; i >= 0; i-- {
			var runner = r.runners[i]
			if err := RunnerClose(ctx, runner); err != nil {
				closeErrors = append(closeErrors, err)
			}
		}
	})
	r.closing.Store(true)
	return errors.Join(closeErrors...)
}

// With applies the pipeline options
func (r *SerialPipeline) With(oo ...PipelineOption) *SerialPipeline {
	r.options.Apply(oo...)
	return r
}

// Start will execute given runner with optional configuration
func Start(ctx context.Context, runner Runner, opts ...Option) error {
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
					err := RunnerClose(closeCtx, runner)
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

	if propagator, ok := runner.(ErrorNotifier); ok {
		go func() {
			select {
			case <-startCtx.Done():
				return
			case <-propagator.Errored():
				go terminate(ctx, true)
			}
		}()
	}

	// runner go routine
	go func() {
		defer chanWriters.Done()
		err := runner.Run(startCtx)
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
	KeepRunningOnError bool
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

// KeepWhenErrored make the pipeline
func KeepRunningOnError() func(*PipelineOptions) {
	return func(o *PipelineOptions) {
		o.KeepRunningOnError = true
	}
}
