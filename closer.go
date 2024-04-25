// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains pipeline closers that are responsible for closing
// gracefully pipeline by external input like os.Signal, task error etc.
package plumber

import (
	"context"
	"os"
	"os/signal"
	"time"
)

// Options represent start pipeline options
type Options struct {
	closers      []func(context.Context) error
	close        func()
	Cancel       func()
	CloseTimeout time.Duration
}

// Apply applies given options into Options struct
func (o *Options) Apply(oo ...Option) *Options {
	for _, op := range oo {
		op(o)
	}
	return o
}

// Closer registers new closer
func (o *Options) Closer(closer func(context.Context) error) *Options {
	o.closers = append(o.closers, closer)
	return o
}

// Close triggers pipeline close
func (o *Options) Close() {
	o.close()
}

// Option type pattern for a Start method
type Option func(*Options)

// Readiness allows to limit time for a RunnerCloser to return from the Run method
// When duration is reached the run context is automatically canceled and the Close method is invoked
func Readiness(d time.Duration) Option {
	return func(*Options) {}
}

// CloseTimeout allows to limit time for a RunnerCloser to return from the Close method
// When duration is reached the close context is automatically canceled
func CloseTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.CloseTimeout = d
	}
}

// TTL is a time duration based closer.
// When duration is reached the closer automatically invokes the Close method
func TTL(d time.Duration) Option {
	return func(o *Options) {
		o.Closer(func(ctx context.Context) error {
			select {
			case <-time.After(d):
				o.Close()
				return nil
			case <-ctx.Done():
				return nil
			}
		})
	}
}

// SignalCloser is os signal based closer
// When any of given signal is received the closer invokes the Close method
func SignalCloser() Option {
	return func(o *Options) {
		o.Closer(func(ctx context.Context) error {
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt)
			// Block until a signal is received.
			select {
			case _, ok := <-c:
				if ok {
					o.Close()
				}
				return nil
			case <-ctx.Done():
				return nil
			}
		})
	}
}

// CloserFunc is a manual close that allows to close runner with provided close function
func CloserFunc(func(close func())) Option {
	return func(*Options) {}
}

// ContextCloser closes the RunnerCloser based on given context.
// When given context is ended the closer invokes the Close method.
// It MUST be used with separate or detached context. See DetachCancellation
func ContextCloser(detachedCtx context.Context) Option {
	return func(o *Options) {
		o.Closer(func(ctx context.Context) error {
			select {
			case <-detachedCtx.Done():
				o.Close()
				return nil
			case <-ctx.Done():
				return nil
			}
		})
	}
}

func Closing(s *ErrorSignaler) Option {
	return func(o *Options) {
		s.Listen(func(err error) {
			o.Close()
		})
	}
}

func Canceling(s *ErrorSignaler) Option {
	return func(o *Options) {
		s.Listen(func(err error) {
			o.Cancel()
		})
	}
}

// ErrorSignaler is a struct providing notifications when task is closed with error.
// Signalers are used usually in combination with Closing | Canceling closers.
type ErrorSignaler struct {
	listeners []func(error)
}

// NewErrorSignaler return a new instance of a error signaler
func NewErrorSignaler() *ErrorSignaler {
	return &ErrorSignaler{}
}

// Signal broadcasts given error to all registered listeners
func (s *ErrorSignaler) Signal(err error) {
	for _, l := range s.listeners {
		l(err)
	}
}

// Listen registers a listener into signaler
func (s *ErrorSignaler) Listen(listener func(err error)) {
	s.listeners = append(s.listeners, listener)
}
