// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains pipeline closers that are responsible for closing
// gracefully pipeline by external input like os.Signal, task error etc.
package plumber

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"
)

// Options represent start pipeline options
type Options struct {
	closers      []func(context.Context) error
	close        func()
	finalizers   []func()
	Cancel       func()
	CloseTimeout time.Duration
}

// Apply applies given options into Options struct
func (o *Options) apply(ctx context.Context, oo ...Option) (out context.Context) {
	for _, op := range oo {
		ctx = op(ctx, o)
	}
	return ctx
}

// closeContext returns ready made close context with or without timeout
func (o *Options) closeContext2(
	ctx context.Context,
	cancelRun context.CancelCauseFunc,
) (out context.Context, cancelAll context.CancelFunc) {
	var cancel context.CancelFunc
	if o.CloseTimeout > 0 {
		out, cancel = context.WithTimeout(ctx, o.CloseTimeout)
	} else {
		out, cancel = context.WithCancel(ctx)
	}

	return out, func() {
		if o.CloseTimeout == 0 {
			cancel()
		}
		<-out.Done()
		cancelRun(out.Err())
	}
}

// closeContext returns ready made close context with or without timeout
func (o *Options) closeContext(
	ctx context.Context,
	cancelRun context.CancelCauseFunc,
) (out context.Context, cancelAll, tryCancel context.CancelFunc) {
	var cancel context.CancelFunc
	if o.CloseTimeout > 0 {
		out, cancel = context.WithTimeout(ctx, o.CloseTimeout)
		tryCancel = func() {}
	} else {
		out, cancel = context.WithCancel(ctx)
		tryCancel = func() {
			fmt.Println("closing all immediately")
			cancel()
		}
	}
	return out, func() {
		<-out.Done()
		cancel()
		cancelRun(out.Err())
		fmt.Println("closing all with", out.Err())
	}, tryCancel
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

// Finalize adds given function to finalizers
// finalizes are invoked when pipeline is closed as a last step
func (o *Options) Finalize(f func()) {
	o.finalizers = append(o.finalizers, f)
}

func (o *Options) finalize() {
	for _, f := range o.finalizers {
		f()
	}
}

// Option type pattern for a Start method
type Option func(context.Context, *Options) context.Context

// Readiness allows to limit time for a Closer to return from the Run method
// When duration is reached the run context is automatically canceled and the Close method is invoked
func ReadinessTimeout(d time.Duration) Option {
	return func(ctx context.Context, o *Options) context.Context {
		return ctx
	}
}

// CloseTimeout allows to limit time for a Closer to return from the Close method
// When duration is reached the close context is automatically canceled
func CloseTimeout(d time.Duration) Option {
	return func(ctx context.Context, o *Options) context.Context {
		o.CloseTimeout = d
		return ctx
	}
}

// TTL is a time duration based closer.
// When duration is reached the closer automatically invokes the Close method
func TTL(d time.Duration) Option {
	return func(ctx context.Context, o *Options) context.Context {
		o.Closer(func(ctx context.Context) error {
			select {
			case <-time.After(d):
				o.Close()
				return nil
			case <-ctx.Done():
				return nil
			}
		})
		return ctx
	}
}

// SignalCloser is OS signal based closer
// When any of given signal is received the closer invokes the Close method
func SignalCloser() Option {
	return func(ctx context.Context, o *Options) context.Context {
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
		return ctx
	}
}

// ChannelCloser is a channel based on channel instance closer
func ChannelCloser(c <-chan struct{}) Option {
	return func(ctx context.Context, o *Options) context.Context {
		o.Closer(func(ctx context.Context) error {
			// Block until a signal is received.
			select {
			case <-c:
				o.Close()
				return nil
			case <-ctx.Done():
				return nil
			}
		})
		return ctx
	}
}

// SignalChannelCloser is a channel based on signal instance
func SignalChannelCloser(s *Signal) Option {
	return ChannelCloser(s.C())
}

// FuncCloser is a manual close that allows to close runner with provided close function by a callback
func FuncCloser(providerCallback func(closeFunc func())) Option {
	return func(ctx context.Context, o *Options) context.Context {
		o.Closer(func(ctx context.Context) error {
			providerCallback(o.Close)
			return nil
		})
		return ctx
	}
}

// ContextCloser closes the Closer based on given context.
// When given context is ended the closer invokes the Close method.
func ContextCloser(parentCtx context.Context) Option {
	return func(ctx context.Context, o *Options) context.Context {
		o.Closer(func(ctx context.Context) error {
			select {
			case <-parentCtx.Done():
				o.Close()
				return nil
			case <-ctx.Done():
				return nil
			}
		})
		return ctx
	}
}

// DetachContext detaches context from cancellation and call close method instead
func DetachContext() Option {
	return func(parentCtx context.Context, o *Options) context.Context {
		detachedContext, cancel := DetachCancellation(parentCtx)
		o.Finalize(func() {
			cancel()
		})
		return ContextCloser(parentCtx)(detachedContext, o)
	}
}
