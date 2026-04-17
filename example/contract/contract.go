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
	// Id
	//
	// plumber.id
	ID int64 `json:"id" yaml:"id"`

	// Name
	//
	// plumber.filter
	Name string `json:"name" yaml:"name"`
}

type Filtrable[T any] struct {
	Value T
}

// Repository describes a database repository
//
// plumber.shape 1
type Repository interface {
	Get(ctx context.Context, id int64) (*Entity, error)
	Create(ctx context.Context, name string) (*Entity, error)
}

// MutatorService describes a mutator service that can create an entities
//
// plumber.shape 1 id=1
type MutatorService interface {
	Create(ctx context.Context, name string) (*Entity, error)
}

// Worker a named worker
//
// plumber:shape ForwardingCloser
// plumber:output {suffix:generated}
// plumber:receiver c
type Closer interface {
	Close(ctx context.Context) (err error)
}

// OpenCloser describes a resource that can be opened and closed
type OpenCloser interface {
	Closer
	Open(ctx context.Context) error
}

type Name string

// Worker a named worker
//
// @macro
//
// plumber:derive
// plumber:name DerivedWorker
// plumber:template tmp1
// plumber:output {suffix:generated}
//
// plumber:derive
// plumber:name WorkerFilter
// plumber:mixin mixing.model.filtrable
// plumber:output {suffix:generated}
//
// plumber:derive
// plumber:mode inplace
// plumber:name WorkerFilterBlended
//
// plumber:shape
// plumber:mixin mixing.model.accessor
type Worker struct {

	// Name of the worker
	//
	// is:filtrable
	Name Name

	Concurrency int

	// CreatedAt is the time when the worker was created
	//
	// is:filtrable
	// is:sortable
	CreatedAt time.Time

	// OpenCloser is a field
	//
	// is:filtrable
	ComplexField OpenCloser

	Queues []string
}

func (r *Worker) SetQueues(value []string) {
	r.Queues = value
}

// @comment
// Neco nekde
//
// plumber:context "github.com/getoutreach/plumber/example/contract".Worker
// plumber:derive WorkerFilterBlended3
// plumber:mode inplace

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
