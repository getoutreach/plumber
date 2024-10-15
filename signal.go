// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Signal to notify when certain even occures.

package plumber

import (
	"context"
	"sync"
)

// Signal is and helper struct that broadcast a notification when certain even occures.
type Signal struct {
	sync.Once
	ch chan struct{}
}

// Notify send a signal to listener. Signal is send just once.
func (s *Signal) Notify() {
	s.Once.Do(
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
		ch: make(chan struct{}),
	}
}

// forwardErrorSignal forward information that a Runner errored to given signal instance
func forwardErrorSignal(ctx context.Context, runner Runner, closed <-chan struct{}, signal *Signal) {
	if notifier, ok := runner.(ErrorNotifier); ok {
		select {
		case <-closed:
			return
		case <-ctx.Done():
			return
		case <-notifier.Errored():
			signal.Notify()
		}
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
