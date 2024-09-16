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

func (s *Signal) Notify() {
	s.Once.Do(
		func() {
			close(s.ch)
		},
	)
}

func (s *Signal) C() <-chan struct{} {
	return s.ch
}

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
