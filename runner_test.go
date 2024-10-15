package plumber_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/getoutreach/plumber"
	"gotest.tools/v3/assert"
)

func TestRunner(t *testing.T) {
	signal := plumber.NewSignal()

	r := plumber.NewRunner(
		func(ctx context.Context) error {
			return nil
		},
		plumber.WithClose(func(ctx context.Context) error {
			return nil
		}),
		plumber.WithReady(signal),
	)

	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)

	go func() {
		time.Sleep(1 * time.Second)
		signal.Notify()
	}()

	defer cancel()

	ready, err := plumber.RunnerReady(r)
	assert.NilError(t, err)

	select {
	case <-ctx.Done():
		fmt.Println("Context")
	case <-ready:
		fmt.Println("Ready")
	}
}
