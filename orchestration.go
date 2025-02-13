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
				ready := RunnerReady(runner)
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

// PipelineRunner creates a pipeline runner so the pipeline it self can be started and closed
func PipelineRunner(runner Runner, opts ...Option) RunnerCloser {
	var (
		signal = NewSignal()
	)
	opts = append(opts, SignalChannelCloser(signal))

	return NewRunner(
		func(ctx context.Context) error {
			err := Start(ctx, runner, opts...)
			return err
		},
		WithClose(func(ctx context.Context) error {
			// trigger close sequence
			signal.Notify()

			return nil
		}),
	)
}

// eventRun is a event run event type for runner state machine
type eventRun struct {
}

// eventRunnerStart is a event start event type for runner state machine
type eventRunnerStart struct {
}

// eventRunnerClose is a event runner close event type for runner state machine
type eventRunnerClose struct {
	id int
}

// eventClose is a event close event type for runner state machine
type eventClose struct {
	closerContext context.Context
	done          chan error
}

// eventFinished is a event finish event type for runner state machine
type eventFinished struct {
	err error
}

// eventError is a event error event type for runner state machine
type eventError struct {
	err error
}

// Start will execute given runner with optional configuration
func Start(xctx context.Context, runner Runner, opts ...Option) error {
	var (
		options   = &Options{}
		messages  = make(chan any, 10)
		onceClose sync.Once
	)

	startCtx := options.apply(xctx, opts...)
	defer options.finalize()

	runCtx, runCancel := context.WithCancelCause(startCtx)

	closers := newCloserContext(runCtx)
	defer closers.cancel()

	options.close = func() {
		onceClose.Do(func() {
			messages <- &eventClose{}
		})
	}

	go closers.startClosers(messages, options.closers...)

	if propagator, ok := runner.(ErrorNotifier); ok {
		go func() {
			select {
			case <-startCtx.Done():
				return
			case <-propagator.Errored():
				onceClose.Do(func() {
					messages <- &eventClose{}
				})
			}
		}()
	}

	// lets try to start the runner
	go func() {
		messages <- &eventRun{}
	}()

	// main event loop
	return errors.Join(func() []error {
		defer func() {
			// drain the close signal
			onceClose.Do(func() {})
		}()
		var running bool
		var closing bool
		errors := []error{}
		for m := range messages {
			switch m := m.(type) {
			// close requested
			case *eventClose:
				// we are not running so we are safe to exit
				if !running {
					return errors
				}
				if closing {
					continue
				}
				closing = true
				go func() {
					closeCtx, closeCancel := options.closeContext(startCtx, runCancel)
					defer closeCancel()
					// start the close sequence
					messages <- &eventError{err: RunnerClose(closeCtx, runner)}
				}()
				// starting runner
			case *eventRun:
				// already started
				if running || closing {
					continue
				}
				running = true
				// runner go routine
				go func() {
					err := runner.Run(runCtx)
					messages <- &eventFinished{err: err}
				}()
				// runner has finished running
			case *eventFinished:
				if m.err != nil {
					errors = append(errors, m.err)
				}
				return errors
				// some error occurred
			case *eventError:
				if m.err != nil {
					errors = append(errors, m.err)
				}
			}
		}
		return errors
	}()...)
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

// startClosers starts closers functions and captures an error
func (c *closerContext) startClosers(messages chan any, closers ...func(context.Context) error) {
	for _, closer := range closers {
		c.erg.Go(func() error {
			return closer(c.ctx)
		})
	}

	// closers go routine
	go func() {
		err := c.erg.Wait()
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			messages <- &eventError{err: err}
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
