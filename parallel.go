// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains parallel pipelines
package plumber

import (
	"context"
	"errors"
	"sync"
)

// ParallelPipeline is a parallel runner closer orchestrator
// The runners are started and closed in concurrent fashion.
// The Run or Close are invoked independently
type ParallelPipeline struct {
	runners []Runner
	options *PipelineOptions

	closed    *Signal
	signal    *Signal
	errSignal *Signal
	messages  chan any
}

// Parallel creates a concurrent Runner executor.
// When started it will execute runners Run and Close methods in parallel.
// Run and Close will block till all runner's corresponding methods are returned.
func Parallel(runners ...Runner) *ParallelPipeline {
	return &ParallelPipeline{
		runners:   runners,
		options:   NewPipelineOptions(),
		signal:    NewSignal(),
		errSignal: NewSignal(),
		closed:    NewSignal(),
		messages:  make(chan any, 10+len(runners)),
	}
}

func (r *ParallelPipeline) Errored() <-chan struct{} {
	return r.errSignal.C()
}

func (r *ParallelPipeline) Ready() <-chan struct{} {
	return r.signal.C()
}

// Run executes Run method on internal runners in parallel.
// It partially implement Runner interface.
// The it returns when all runner's Run methods are returned.
func (r *ParallelPipeline) Run(ctx context.Context) error {
	// lets try to start the runner
	go func() {
		r.messages <- &eventRun{}
	}()

	// main event loop
	return errors.Join(func() []error {
		var readyRunners int
		var finishedRunners int
		var running bool
		var closing bool
		var closeDone chan error
		var closeOnce sync.Once
		var closeContext = context.Background()

		errs := []error{}
		for m := range r.messages {
			switch m := m.(type) {
			case *eventReady:
				readyRunners++
				// We are all ready
				if readyRunners == len(r.runners) {
					r.signal.Notify()
				}
			case *eventClose:
				if !running {
					return errs
				}
				if closing {
					if m.done != nil {
						close(m.done)
					}
					continue
				}
				closing = true
				r.closed.Notify()

				// ensure we have a close context
				if m.closerContext != nil {
					closeContext = m.closerContext
				}
				closeDone = m.done
				go r.closeAll(closeContext)
			case *eventClosed:
				closeOnce.Do(func() {
					if closeDone != nil {
						closeDone <- m.err
					}
					close(closeDone)
				})
			case *eventRun:
				if running || closing {
					continue
				}
				running = true
				for _, runner := range r.runners {
					go r.run(ctx, runner)
				}
			case *eventFinished:
				finishedRunners++
				if m.err != nil {
					errs = append(errs, m.err)
				}
				if finishedRunners == len(r.runners) {
					return errs
				}
			}
		}
		return errs
	}()...)
}

// run start a single runner
func (r *ParallelPipeline) run(ctx context.Context, runner Runner) {
	// Wait for the runner to be ready
	go func() {
		ready := RunnerReady(runner)
		select {
		case <-ready:
			r.messages <- &eventReady{}
		case <-ctx.Done():
		}
	}()

	go r.options.ErrorNotifier.Forward(ctx, runner, r.closed, r.errSignal)

	err := runner.Run(ctx)

	if err != nil {
		r.options.ErrorNotifier.Notify(r.errSignal)
	}
	r.messages <- &eventFinished{err: err}
}

// closeAll closes all runners
func (r *ParallelPipeline) closeAll(ctx context.Context) {
	closeErrors := []error{}
	var wg sync.WaitGroup
	wg.Add(len(r.runners))

	for _, runner := range r.runners {
		go func(runner Runner) {
			defer wg.Done()
			if err := RunnerClose(ctx, runner); err != nil {
				closeErrors = append(closeErrors, err)
			}
		}(runner)
	}
	wg.Wait()
	r.messages <- &eventClosed{err: errors.Join(closeErrors...)}
}

// Close executes Close method on internal runners in revered order to achieve graceful shutdown sequence
// It implements Closer interface
func (r *ParallelPipeline) Close(ctx context.Context) error {
	event := &eventClose{
		closerContext: ctx,
		done:          make(chan error, 1),
	}
	r.messages <- event
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-event.done:
		return err
	}
}

// With applies the pipeline options
func (r *ParallelPipeline) With(oo ...PipelineOption) *ParallelPipeline {
	r.options.apply(oo...)
	return r
}
