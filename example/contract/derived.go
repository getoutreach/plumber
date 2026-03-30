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

func test(ctx context.Context) {

}
