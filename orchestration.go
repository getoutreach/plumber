// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains task orchestration code
package plumber

import (
	"context"
	"errors"
	"fmt"
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

// WaitTill waits till channel is not close or till given context is ended and then returns all accumulated errors.
func (ec ErrorCh) WaitTill(ctx context.Context) []error {
	var (
		errs = []error{}
	)
	for {
		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.Canceled) {
				errs = append(errs, ctx.Err())
			}
			return errs
		case err, ok := <-ec:
			if err != nil {
				errs = append(errs, err)
			}
			if !ok {
				return errs
			}
		}
	}
}

// Wait waits till channel is not close and returns all accumulated errors.
func (ec ErrorCh) Wait() []error {
	var (
		errs = []error{}
	)
	for {
		err, ok := <-ec
		if err != nil {
			errs = append(errs, err)
		}
		if !ok {
			return errs
		}
	}
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
	r.options.apply(oo...)
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

// Serial creates a serial Runner executor.
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

	err := errors.Join(errs.Errors()...)
	return err
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
	r.options.apply(oo...)
	return r
}

// PipelineRunner creates a pipeline runner so the pipeline it self can be started and closed
func PipelineRunner(runner Runner, opts ...Option) RunnerCloser {
	var (
		signal = NewSignal()
		//		wait   = make(chan struct{}, 1)
	)
	opts = append(opts, SignalChannelCloser(signal))

	return NewRunner(
		func(ctx context.Context) error {
			err := Start(ctx, runner, opts...)
			//			close(wait)
			return err
		},
		WithClose(func(ctx context.Context) error {
			// trigger close sequence
			signal.Notify()
			// select {
			// case <-wait:
			// case <-ctx.Done():
			// }
			return nil
		}),
	)
}

// Start will execute given runner with optional configuration
func Start(xctx context.Context, runner Runner, opts ...Option) error {
	var (
		options = &Options{}
	)

	startCtx := options.apply(xctx, opts...)
	defer options.finalize()

	runCtx, runCancel := context.WithCancelCause(startCtx)
	defer runCancel(errors.New("start context is canceled"))

	closers := newCloserContext(startCtx)
	defer closers.cancel()

	var (
		errorCh       = make(ErrorCh, 3)
		finishedCh    = make(chan struct{}, 1)
		once          sync.Once
		chanWriters   sync.WaitGroup
		closeChannels = func() {
			closers.cancel()
		}

		terminate = func(initiateClose bool) {
			once.Do(func() {
				if !initiateClose {
					closeChannels()
					return
				}
				closeCtx, closeCancel, tryCloseCancel := options.closeContext(startCtx, runCancel)
				defer closeCancel()

				// go routine handling close async so it can be canceled
				chanWriters.Add(1)
				go func() {
					defer chanWriters.Done()
					err := RunnerClose(closeCtx, runner)
					tryCloseCancel()
					errorCh <- err
					closeChannels()
				}()
				chanWriters.Add(1)
				defer chanWriters.Done()
				select {
				case <-finishedCh:
					closeCancel()
				case <-closeCtx.Done():
					runCancel(fmt.Errorf("closeCtx: %w", closeCtx.Err()))
					if err := closeCtx.Err(); err != nil {
						errorCh <- fmt.Errorf("closeCtx: %w", err)
					}
				}
			})
		}
	)
	chanWriters.Add(2)

	go func() {
		chanWriters.Wait()
		// We can really terminate since all channel writers are done
		close(errorCh)
	}()

	options.close = func() {
		chanWriters.Add(1)
		go func() {
			defer chanWriters.Done()
			terminate(true)
		}()
	}

	closers.start(errorCh, &chanWriters, options.closers...)

	if propagator, ok := runner.(ErrorNotifier); ok {
		go func() {
			select {
			case <-startCtx.Done():
				return
			case <-propagator.Errored():
				go terminate(true)
			}
		}()
	}

	// runner go routine
	go func() {
		defer chanWriters.Done()
		err := runner.Run(runCtx)
		if err != nil {
			// runner sequence had a problems calling close
			errorCh <- err
		}
		close(finishedCh)
		go terminate(false)
	}()

	return errors.Join(errorCh.Wait()...)
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

// apply given PipelineOption
func (o *PipelineOptions) apply(oo ...PipelineOption) {
	for _, op := range oo {
		op(o)
	}
}

// KeepWhenErrored make the pipeline
func KeepRunningOnError() func(*PipelineOptions) {
	return func(o *PipelineOptions) {
		o.KeepRunningOnError = true
	}
}

// UnlessCanceled returns nil if error is context.Canceled
// The pipeline might return context.Canceled error when it is closed
func UnlessCanceled(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
