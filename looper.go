// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains looper structs
package plumber

import "context"

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

func NewBaseLooper(looper func(ctx context.Context, loop *Loop) error) *BaseLooper {
	return &BaseLooper{
		runner: Looper(looper),
	}
}
