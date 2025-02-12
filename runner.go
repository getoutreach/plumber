// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Runner interfaces
package plumber

import (
	"context"
	"sync"
)

// Runner describes basic runnable unit. Runner can be started.
// This interface is used as a common denominator used in api but,
// it is more that recommended to implement other interface Closable
// and optionally Readyable.
type Runner interface {
	Run(ctx context.Context) error
}

// Readier describes Runner that can signal whether it is ready.
// This is useful when Runners needs to be execute with the Pipeline sequentially.
type Readier interface {
	Ready() (<-chan struct{}, error)
}

// Closeable describes Runner that can be graceful closed. Close method must be idempotent.
// Once called runner is required exit from main Run method within defined duration otherwise run context will be canceled.
// Close can block for configured duration. When exceeded close context is canceled
type Closeable interface {
	Close(ctx context.Context) error
}

// ErrorNotifier describes Runner that can report that error has occurred before it returns from the main Run method.
// This is signal might bubble up the execution tree and on top it might be used to start Close sequence.
type ErrorNotifier interface {
	Errored() <-chan struct{}
}

// RunnerCloser implements Runner and Closeable interfaces
type RunnerCloser interface {
	Runner
	Closeable
}

// SmartRunner implements all interfaces that makes the runner good citizen
type SmartRunner interface {
	Runner
	Readier
	Closeable
}

// RunnerOptions holds runner optional callbacks
type RunnerOptions struct {
	close func(ctx context.Context) error
	ready func() <-chan struct{}
}

func (o *RunnerOptions) apply(opts ...RunnerOption) {
	o.ready = func() <-chan struct{} {
		return closedCh
	}
	for _, op := range opts {
		op(o)
	}
}

// RunnerOption is option pattern function
type RunnerOption func(*RunnerOptions)

func WithReady(s *Signal) RunnerOption {
	return func(ro *RunnerOptions) {
		ro.ready = func() <-chan struct{} {
			return s.C()
		}
	}
}

func WithClose(closeFunc func(context.Context) error) RunnerOption {
	return func(ro *RunnerOptions) {
		ro.close = closeFunc
	}
}

// runner represent a struct that complies with Runner interfaces
type runner struct {
	options RunnerOptions
	run     func(ctx context.Context) error
	mx      sync.Mutex
}

// NewRunner returns an instance of the runner. Optionally supplied options might redefine other Runner method Close and Ready
func NewRunner(run func(ctx context.Context) error, opts ...RunnerOption) SmartRunner {
	r := &runner{
		run: run,
	}
	r.options.apply(opts...)
	return r
}

// Ready signals that runner is ready
func (r *runner) Ready() (<-chan struct{}, error) {
	return r.options.ready(), nil
}

// Run executes a task
func (r *runner) Run(ctx context.Context) error {
	return r.run(ctx)
}

// Close runner. Once called runner is required to exit from main Run method
// within defined duration otherwise run context will be canceled.
// Close can block for configured duration. When exceeded close context is canceled
func (r *runner) Close(ctx context.Context) error {
	if r.options.close != nil {
		return r.options.close(ctx)
	}
	return nil
}

// GracefulRunner is runner supporting Run and Close methods
func GracefulRunner(run, closeFn func(ctx context.Context) error) Runner {
	return NewRunner(run, WithClose(closeFn))
}

// closedCh is a ready made closed channel
var closedCh = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// RunnerReady return channel that can be used to check if runner is ready.
// When channel is closed runner can be considered ready.
func RunnerReady(runner Runner) (<-chan struct{}, error) {
	if r, ok := runner.(Readier); ok {
		return r.Ready()
	}
	return closedCh, nil
}

// RunnerClose calls Close method on given Runner when supported
func RunnerClose(ctx context.Context, runner Runner) error {
	if r, ok := runner.(Closeable); ok {
		return r.Close(ctx)
	}
	return nil
}
