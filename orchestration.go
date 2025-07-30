// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains task orchestration code
package plumber

import (
	"context"
	"errors"
	"fmt"
	"sync"

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

// Finish reports back runner status using given error
func (f DoneFunc) Finish(err error) {
	if f != nil {
		f(err)
	}
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
// run method will block until context is done or close function is invoked
func Closer(closeFunc CallbackFunc) Runner {
	signal := NewSignal()
	return NewRunner(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-signal.C():
			return nil
		}
	}, WithClose(func(ctx context.Context) error {
		err := closeFunc(ctx)
		signal.Notify()
		return err
	}))
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
			fmt.Println("PipelineRunner closing")
			// trigger close sequence
			signal.Notify()

			return nil
		}),
	)
}

// eventRun is a event run event type for runner state machine
type eventRun struct {
}

// eventReady is a event runner ready event type for runner state machine
type eventReady struct {
}

// eventClosed is a event all runner closed  event type for runner state machine
type eventClosed struct {
	err error
}

// eventRunnerStart is a event start event type for runner state machine
type eventRunnerStart struct {
}

// eventRunnerClose is a event runner close event type for runner state machine
type eventRunnerClose struct {
	id  int
	err error
}

// eventClose is a event close event type for runner state machine
type eventClose struct {
	closerContext context.Context
	done          chan error
	terminate     bool
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
		errs := []error{}
		for m := range messages {
			switch m := m.(type) {
			// close requested
			case *eventClose:
				// we are not running so we are safe to exit
				if !running {
					return errs
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
					// notify about pipeline readiness
					if options.readySignal != nil {
						ready := RunnerReady(runner)
						go func() {
							select {
							case <-ready:
								options.readySignal.Notify()
							case <-runCtx.Done():
							}
						}()
					}
					err := runner.Run(runCtx)
					messages <- &eventFinished{err: err}
				}()
				// runner has finished running
			case *eventFinished:
				if m.err != nil {
					errs = append(errs, m.err)
				}
				return errs
				// some error occurred
			case *eventError:
				if m.err != nil {
					errs = append(errs, m.err)
				}
			}
		}
		return errs
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
	ErrorNotifier      ErrorNotifierStrategy
	KeepRunningOnError bool
	CloseNotRunning    bool
}

// NewPipelineOptions creates a default instance of PipelineOptions
func NewPipelineOptions() *PipelineOptions {
	return &PipelineOptions{
		ErrorNotifier: NotifyingErrorNotifier{},
	}
}

// PipelineOption is a option patter struct for PipelineOptions
type PipelineOption func(*PipelineOptions)

// apply given PipelineOption
func (o *PipelineOptions) apply(oo ...PipelineOption) {
	for _, op := range oo {
		op(o)
	}
}

// WithErrorNotifier overrides default error notifier strategy
func WithErrorNotifier(errorNotifier ErrorNotifierStrategy) func(*PipelineOptions) {
	return func(o *PipelineOptions) {
		o.ErrorNotifier = errorNotifier
	}
}

// CloseNotRunning makes sure that close sequence will be executed even if the runner hasn't been started
// This is useful for pipelines including runners that works as cleanup tasks that needs to be executed all the time
func CloseNotRunning() func(*PipelineOptions) {
	return func(o *PipelineOptions) {
		o.CloseNotRunning = true
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
