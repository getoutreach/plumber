// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains a compatibility layer with https://github.com/getoutreach/gobox/blob/main/pkg/async/async.go
package plumber

import (
	"context"
	"io"
)

// AsyncRunner provides a compability adapter with async.Runner interface
func AsyncRunner(runner interface {
	Run(ctx context.Context) error
}) RunnerCloser {
	type Closer interface {
		Close(ctx context.Context) error
	}
	return GracefulRunner(func(ctx context.Context, ready ReadyFunc) error {
		go ready()
		return runner.Run(ctx)
	}, func(ctx context.Context) error {
		switch r := runner.(type) {
		case Closer:
			return r.Close(ctx)
		case io.Closer:
			return r.Close()
		}
		return nil
	})
}
