// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains derived/blended struct types generated from Worker contract types for the example package.

package contract

import (
	"context"
	"time"
)

// DerivedWorker is derived from "github.com/getoutreach/plumber/example/contract".Worker.
type WorkerFilterBlended struct {
	Name         string
	Concurrency  int
	CreatedAt    time.Time
	ComplexField OpenCloser
	Queues       []string
}

type WorkerFilterBlended3 struct {
	Name         string
	Concurrency  int
	CreatedAt    time.Time
	ComplexField OpenCloser
	Queues       []string
}

func test(ctx context.Context) {

}
