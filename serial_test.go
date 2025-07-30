package plumber_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getoutreach/plumber"
	"gotest.tools/v3/assert"
)

func TestCloseIsCalledWhenOneFails(t *testing.T) {
	var (
		called atomic.Bool
	)
	ctx := context.Background()
	err := plumber.Start(ctx,
		plumber.Pipeline(
			erroringLazyRunner("worker #3"),
			plumber.Closer(func(ctx context.Context) error {
				called.Store(true)
				return nil
			}),
		),
		plumber.TTL(2*time.Second),
	)
	assert.Error(t, err, "runner worker #3 failed")
	assert.Assert(t, called.Load(), "Close should be called when one of the runners fails")
}

func TestCloseIsCalledWhenOneFailsAndNotStarted(t *testing.T) {
	var (
		called atomic.Bool
	)
	ctx := context.Background()
	err := plumber.Start(ctx,
		plumber.Pipeline(
			erroringRunner("worker #1"),
			erroringRunner("worker #2"),
			plumber.Closer(func(ctx context.Context) error {
				called.Store(true)
				return nil
			}),
		).With(plumber.CloseNotRunning()),
		plumber.TTL(2*time.Second),
	)
	assert.ErrorContains(t, err, "runner worker #1 failed")
	assert.Assert(t, called.Load(), "Close should be called when one of the runners fails")
}

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
