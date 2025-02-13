package plumber_test

import (
	"context"
	"time"

	"github.com/getoutreach/plumber"
)

// ExamplePipeline demonstrates how serial pipeline is handling workers and how closing sequence looks like
// it closes all workers in reversed order and waiting for each worker to return from it Run method before closing next one
func ExamplePipeline() {
	ctx := context.Background()

	closed := make(chan time.Time, 3)

	err := plumber.Start(ctx,
		plumber.Pipeline(
			reportingLooperRunner("worker #1"),
			reportingLooperRunner("worker #2"),
			reportingLooperRunner("worker #3"),
		),
		plumber.TTL(1*time.Second),
	)
	if err != nil {
		panic(err)
	}
	close(closed)
	// Output:
	// runner[ worker #1 ] starting
	// runner[ worker #1 ] working
	// runner[ worker #2 ] starting
	// runner[ worker #2 ] working
	// runner[ worker #3 ] starting
	// runner[ worker #3 ] working
	// runner[ worker #3 ] closing
	// runner[ worker #3 ] finished
	// runner[ worker #2 ] closing
	// runner[ worker #2 ] finished
	// runner[ worker #1 ] closing
	// runner[ worker #1 ] finished
}
