// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains serial pipelines
package plumber

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/samber/lo"
)

// runningRunner is a helper struct to keep track of running runners
type runningRunner struct {
	runner Runner
	id     int
}

// SerialPipeline is a serial runner closer orchestrator
// The runners are started and closed in serial fashion.
// The Run or Close methods needs to return and only then next runner is evaluated
type SerialPipeline struct {
	runners   []Runner
	options   *PipelineOptions
	ready     *Signal
	errSignal *Signal

	messages chan any
}

// Pipeline creates a serial Runner executor.
// When started it will execute Run method on given runners one by one with given order.
// When closed it will execute Close method on given runners in revered order to achieve graceful shutdown sequence
func Pipeline(runners ...Runner) *SerialPipeline {
	return &SerialPipeline{
		runners:   runners,
		options:   &PipelineOptions{},
		ready:     NewSignal(),
		errSignal: NewSignal(),

		messages: make(chan any, 10),
	}
}

func (r *SerialPipeline) Errored() <-chan struct{} {
	return r.errSignal.C()
}

func (r *SerialPipeline) Ready() <-chan struct{} {
	return r.ready.C()
}

// Run executes Run method on internal runners one by one with given order.
func (r *SerialPipeline) Run(ctx context.Context) error {
	runCtx, runCancel := context.WithCancelCause(ctx)
	defer runCancel(nil)

	// lets try to start the runner
	go func() {
		r.messages <- &eventRun{}
	}()

	// main event loop
	return errors.Join(func() []error {
		var closeErrors []error
		var running bool
		var closing bool
		var workerID int
		var closeContext = context.Background()
		var runningWorkers []runningRunner
		var closeDone chan error
		var closeOnce sync.Once
		errs := []error{}
		for m := range r.messages {
			switch m := m.(type) {
			// close requested
			case *eventClose:
				if !running {
					return errs
				}
				if closing {
					continue
				}
				closing = true
				// We have nothing to close
				if len(runningWorkers) == 0 {
					return errs
				}

				// ensure we have a close context
				if m.closerContext != nil {
					closeContext = m.closerContext
				}
				closeDone = m.done

				r.messages <- &eventRunnerClose{}
				// starting runner
			case *eventRun:
				if running || closing {
					continue
				}
				running = true
				r.messages <- &eventRunnerStart{}
				// runner has finished running
			case *eventRunnerClose:
				// filter out the one that has ended
				if m.id > 0 {
					runningWorkers = lo.Filter(runningWorkers, func(e runningRunner, _ int) bool {
						return e.id != m.id
					})
				}

				// if we still have runners to close we keep closing
				if len(runningWorkers) > 0 {
					last := runningWorkers[len(runningWorkers)-1]
					// we can call synchronously close since we are anyway waiting for run to finish
					closeErrors = append(closeErrors, RunnerClose(closeContext, last.runner))
				}

				if len(runningWorkers) == 0 {
					closeOnce.Do(func() {
						if closeDone == nil {
							return
						}
						closeDone <- errors.Join(closeErrors...)
						close(closeDone)
					})
					return errs
				}
			case *eventRunnerStart:
				workerID++
				if closing || workerID > len(r.runners) {
					if !closing {
						// We are all ready and running
						r.ready.Notify()
					}
					// we are all running or closing
					continue
				}
				func(id int) {
					runner := runningRunner{
						runner: r.runners[id-1],
						id:     id,
					}

					runningWorkers = append(runningWorkers, runner)
					go func(running runningRunner) {
						// Wait for the runner to become ready and then signal to start another runner
						ready := RunnerReady(running.runner)
						go func() {
							select {
							case <-ready:
								r.messages <- &eventRunnerStart{}
							case <-runCtx.Done():
							}
						}()

						err := running.runner.Run(runCtx)

						r.messages <- &eventError{err: err}
						// we can close another runner or start closing the pipeline
						r.messages <- &eventRunnerClose{id: running.id}
					}(runner)
				}(workerID)
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

// Close executes Close method on internal runners in revered order to achieve graceful shutdown sequence
// It implements Closer interface
func (r *SerialPipeline) Close(ctx context.Context) error {
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
func (r *SerialPipeline) With(oo ...PipelineOption) *SerialPipeline {
	r.options.apply(oo...)
	return r
}

// SerialNonBlockingPipeline is a serial runner closer orchestrator
// The runners are started and closed in serial fashion.
// NonBlocking version does not wait for the runner to finish before closing the next one
type SerialNonBlockingPipeline struct {
	runners   []Runner
	options   *PipelineOptions
	closing   atomic.Bool
	closed    chan struct{}
	closeOnce sync.Once
	signal    *Signal
	errSignal *Signal
}

// PipelineNonBlocking creates a non blocking serial Runner executor.
// When started it will execute Run method on given runners one by one with given order.
// When closed it will execute Close method on given runners in revered order to achieve graceful shutdown sequence
// NonBlocking version does not wait for the runner to finish before closing the next one
func PipelineNonBlocking(runners ...Runner) *SerialNonBlockingPipeline {
	return &SerialNonBlockingPipeline{
		runners:   runners,
		options:   &PipelineOptions{},
		closed:    make(chan struct{}),
		signal:    NewSignal(),
		errSignal: NewSignal(),
	}
}

func (r *SerialNonBlockingPipeline) Errored() <-chan struct{} {
	return r.errSignal.C()
}

func (r *SerialNonBlockingPipeline) Ready() <-chan struct{} {
	return r.signal.C()
}

// Run executes Run method on internal runners one by one with given order.
func (r *SerialNonBlockingPipeline) Run(ctx context.Context) error {
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

							ready := RunnerReady(runner)

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
func (r *SerialNonBlockingPipeline) Close(ctx context.Context) error {
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
func (r *SerialNonBlockingPipeline) With(oo ...PipelineOption) *SerialNonBlockingPipeline {
	r.options.apply(oo...)
	return r
}
