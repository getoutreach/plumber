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
	err := plumber.Start(ctx,
		plumber.Pipeline(
			plumber.Pipeline(
				reportingLooperRunner("worker #1"),
				reportingLooperRunner("worker #2"),
				reportingLooperRunner("worker #3"),
			),
			reportingLooperRunner("worker #4"),
		),
		plumber.TTL(1*time.Second),
	)
	if err != nil {
		panic(err)
	}
	// Output:
	// runner[ worker #1 ] starting
	// runner[ worker #1 ] working
	// runner[ worker #2 ] starting
	// runner[ worker #2 ] working
	// runner[ worker #3 ] starting
	// runner[ worker #3 ] working
	// runner[ worker #4 ] starting
	// runner[ worker #4 ] working
	// runner[ worker #4 ] closing
	// runner[ worker #4 ] finished
	// runner[ worker #3 ] closing
	// runner[ worker #3 ] finished
	// runner[ worker #2 ] closing
	// runner[ worker #2 ] finished
	// runner[ worker #1 ] closing
	// runner[ worker #1 ] finished
}
