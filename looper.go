// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains looper structs
package plumber

import (
	"context"
	"sync"
)

// BaseLooper is a looper struct that can be used in composition as following example:
//
//	type Looper struct {
//		    *plumber.BaseLooper
//	}
//
//	s := &Looper{}
//	s.BaseLooper = plumber.NewBaseLooper(s.loop)
//
// func (s *Looper) loop(ctx context.Context, l *plumber.Loop) error {
// ....
// }
type BaseLooper struct {
	runner Runner
}

// Run executes runners workload. Pipelines are starting Run method in separated goroutine.
// Runner must report its readiness using given callback
func (l *BaseLooper) Run(ctx context.Context) error {
	return l.runner.Run(ctx)
}

// Close method triggers graceful shutdown on the task. It should block till task is properly closed.
// When Close timeout is exceeded then given context is canceled.
func (l *BaseLooper) Close(ctx context.Context) error {
	return RunnerClose(ctx, l.runner)
}

// Ready signals that runner is ready
func (l *BaseLooper) Ready() <-chan struct{} {
	return RunnerReady(l.runner)
}

func NewBaseLooper(looper func(ctx context.Context, loop *Loop) error) *BaseLooper {
	return &BaseLooper{
		runner: Looper(looper),
	}
}

// Loop is a looper controlling struct
type Loop struct {
	closeCh chan DoneFunc
	ready   ReadyFunc
}

// Ready reports runner's readiness
func (l *Loop) Ready() {
	if l.ready != nil {
		l.ready()
	}
}

// Closing returns a channel that's closed when cancellation is requested
func (l *Loop) Closing() <-chan DoneFunc {
	return l.closeCh
}

// Looper creates a runner that runs in the detached go routine in indefinite loop.
// Provided Loop struct allows to detect a cancellation and graceful termination
// Example:
//
//	plumber.Looper(func(ctx context.Context, l *plumber.Loop) error {
//	    l.Ready()
//	    tick := time.Tick(500 * time.Millisecond)
//	    for {
//	        select {
//	        case <-tick:
//	            // Work
//	            fmt.Println("Work")
//	        case closeDone := <-l.Closing():
//	            fmt.Println("Close is requested")
//	            closeDone.Success()
//	            // Graceful shutdown
//	            return nil
//	        case <-ctx.Done():
//	            // Cancel / Timeout
//	            return ctx.Err()
//	        }
//	    }
//	})
func Looper(run func(ctx context.Context, loop *Loop) error) RunnerCloser {
	var (
		runOnce    sync.Once
		closeOnce  sync.Once
		returnedCh = make(chan struct{}, 1)
		l          = &Loop{
			closeCh: make(chan DoneFunc, 1),
		}
	)

	signal := NewSignal()

	return NewRunner(
		func(ctx context.Context) error {
			var err error
			runOnce.Do(func() {
				l.ready = func() {
					signal.Notify()
				}
				defer closeOnce.Do(func() {
					close(l.closeCh)
				})
				err = run(ctx, l)
				close(returnedCh)
			})
			return err
		},
		WithClose(func(ctx context.Context) error {
			var (
				errCh             = make(chan error, 1)
				canceled DoneFunc = func(err error) {
					errCh <- err
					close(errCh)
				}
			)
			// if hasn't been started, lets close it
			closeOnce.Do(func() {
				l.closeCh <- canceled
				close(l.closeCh)
			})
			select {
			case <-returnedCh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case err := <-errCh:
				return err
			}
		}),
		WithReady(signal),
	)
}
