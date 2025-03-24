// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file restartable infra form runners

// Package restart provides a way to create runners that are automatically restarted in case of error
package restart

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/getoutreach/plumber"
)

// InstanceableRunner is a runner that can create instances and instance can be run
type InstanceableRunner[O plumber.Runner] interface {
	MakeInstanceError() (O, error)
}

// Always is a runner that is automatically restarted in case of error
// Unless the error is of type NonRestartableError
func Always() RestartOption {
	return func(o *RestartOptions) {
		o.ErrDecider = func(e error) bool {
			return !IsNonRestartableError(e)
		}
	}
}

// WithDelay defines an delay before restarting the runner
func WithDelay(d time.Duration) RestartOption {
	return func(o *RestartOptions) {
		o.Delay = d
	}
}

// RestartOptions is the options for the restartable runner
type RestartOptions struct {
	// ErrDecider is a function that decides if the error should be restarted
	ErrDecider func(error) bool
	// Delay is the time to wait before restarting
	Delay time.Duration
}

// RestartOption is a function that sets the options for the restartable runner
type RestartOption func(o *RestartOptions)

// Restartable creates a runner that is automatically restarted in case of error
func Restartable[T plumber.Runner](makeInstance InstanceableRunner[T], options ...RestartOption) plumber.Runner {
	o := &RestartOptions{}
	Always()(o)
	for _, option := range options {
		option(o)
	}
	stopDelay := plumber.NewSignal()
	return plumber.Looper(func(ctx context.Context, l *plumber.Loop) error {
		l.Ready()

		errorCh := make(chan error, 100)

		restart := func(errs chan error) plumber.Runner {
			runner, err := makeInstance.MakeInstanceError()
			if err != nil {
				errs <- NewNonRestartableError(err)
				return nil
			}
			go func() {
				err := runner.Run(ctx)
				if err != nil && o.Delay > 0 {
					select {
					case <-time.After(o.Delay):
						// noop
					case <-stopDelay.C():
						// noop
					}
				}
				errs <- err
			}()
			return runner
		}

		var (
			runningRunner plumber.Runner
			closing       bool
		)

		runningRunner = restart(errorCh)

		// Stop delay when main loop is done
		defer stopDelay.Notify()

		for {
			select {
			case closeDone := <-l.Closing():
				closing = true

				closeDone.Finish(plumber.RunnerClose(ctx, runningRunner))

				// Stop delay when closing as well
				stopDelay.Notify()
			case <-ctx.Done():
				// Cancel / Timeout
				return ctx.Err()
			case err := <-errorCh:
				if closing {
					return err
				}
				if IsNonRestartableError(err) || !o.ErrDecider(err) {
					return err
				}
				runningRunner = restart(errorCh)
			}
		}
	})
}

// NonRestartableError is an error that should not be restarted
type NonRestartableError struct {
	error
}

// NewNonRestartableError is a factory method for NewNonRestartableError
func NewNonRestartableError(err error) error {
	return &NonRestartableError{err}
}

// IsNonRestartableError checks if there is any error in the chain of type NewNonRestartableError
func IsNonRestartableError(err error) bool {
	var re *NonRestartableError
	return errors.As(err, &re)
}

// Unwrap returns the underlying error.
// This method is required by errors.Unwrap.
func (e *NonRestartableError) Unwrap() error {
	return e.error
}

// Error returns a text message of the error
func (e *NonRestartableError) Error() string {
	return fmt.Sprintf("nonrestartable: %s", e.error.Error())
}
