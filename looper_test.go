package plumber_test

import (
	"context"
	"fmt"
	"time"

	"github.com/getoutreach/plumber"
)

func ExampleLooper() {
	ctx := context.Background()
	err := plumber.Start(ctx,
		plumber.Looper(func(ctx context.Context, l *plumber.Loop) error {
			l.Ready()
			tick := time.Tick(500 * time.Millisecond)
			fmt.Println("Looper starting up")
			for {
				select {
				case <-tick:
					// Work
					fmt.Println("Looper work")
				case done := <-l.Closing():
					fmt.Println("Looper requested to shutdown")
					done.Success()
					fmt.Println("Looper finished")
					// Graceful shutdown
					return nil
				case <-ctx.Done():
					fmt.Println("Looper canceled")
					// Cancel / Timeout
					return ctx.Err()
				}
			}
		}),
		plumber.TTL(600*time.Millisecond),
		plumber.CloseTimeout(3*time.Second),
	)
	if err != nil {
		fmt.Printf("err: %v", err)
	}
	// Output:
	// Looper starting up
	// Looper work
	// Looper requested to shutdown
	// Looper finished
}
