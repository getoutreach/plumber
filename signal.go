// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Signal to notify when certain even occures.

package plumber

import (
	"context"
	"sync"
)

// Signal is and helper struct that broadcast a notification when certain even occures.
type Signal struct {
	once sync.Once
	ch   chan struct{}
}

// Notify send a signal to listener. Signal is send just once.
func (s *Signal) Notify() {
	s.once.Do(
		func() {
			close(s.ch)
		},
	)
}

// C returns a channel that can be used to received
func (s *Signal) C() <-chan struct{} {
	return s.ch
}

// NewSignal returns a signal notifier
func NewSignal() *Signal {
	return &Signal{
		ch: make(chan struct{}, 1),
	}
}

// closeOnError when given signal reports error that runner is closed
func closeOnError(ctx context.Context, signal *Signal, runner Closeable) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-signal.C():
			runner.Close(ctx)
		}
	}()
}

// ErrorNotifierStrategy is a strategy to notify about errors
type ErrorNotifierStrategy interface {
	Forward(ctx context.Context, runner Runner, closed, signal *Signal)
	Notify(signal *Signal)
}

// NotifyingErrorNotifier is a strategy that forwards error notifications
type NotifyingErrorNotifier struct{}

// Forward forwards error notifications
func (NotifyingErrorNotifier) Forward(ctx context.Context, runner Runner, closed, signal *Signal) {
	if notifier, ok := runner.(ErrorNotifier); ok {
		select {
		case <-closed.C():
			return
		case <-ctx.Done():
			return
		case <-notifier.Errored():
			signal.Notify()
		}
	}
}

// Notify notifies about errors
func (NotifyingErrorNotifier) Notify(signal *Signal) {
	signal.Notify()
}

// NoopErrorNotifier is a strategy that does not notify about the errors
type NoopErrorNotifier struct{}

// Forward forwards error notifications
func (NoopErrorNotifier) Forward(ctx context.Context, runner Runner, closed, signal *Signal) {}

// Notify notifies about errors
func (NoopErrorNotifier) Notify(signal *Signal) {}
