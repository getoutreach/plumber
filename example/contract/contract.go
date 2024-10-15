// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: contract for example application

// Package contract provides contract for example application
package contract

import (
	"context"
	"fmt"
	"time"

	"github.com/getoutreach/plumber"
)

// Entity represents example entity
type Entity struct {
	ID   int64
	Name string
}

// Repository describes a database repository
type Repository interface {
	Get(ctx context.Context, id int64) (*Entity, error)
	Create(ctx context.Context, name string) (*Entity, error)
}

// MutatorService describes a mutator service that can create an entities
type MutatorService interface {
	Create(ctx context.Context, name string) (*Entity, error)
}

// Worker a named worker
type Worker struct {
	Name string
}

// NewWorker return instance of the worker
func NewWorker(name string) *plumber.BaseLooper {
	return plumber.NewBaseLooper(func(ctx context.Context, l *plumber.Loop) error {
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("[%s] starting up\n", name)
		l.Ready()
		tick := time.NewTicker(1000 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				// Work
				fmt.Printf("[%s] work\n", name)
			case done := <-l.Closing():
				fmt.Printf("[%s] requested to shutdown\n", name)
				done.Success()
				fmt.Printf("[%s] finished\n", name)
				// Graceful shutdown
				return nil
			case <-ctx.Done():
				fmt.Printf("[%s] canceled\n", name)
				// Cancel / Timeout
				return ctx.Err()
			}
		}
	})
}
