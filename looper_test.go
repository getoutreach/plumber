package plumber_test

import (
	"context"
	"fmt"
	"time"

	"github.com/outreach/plumber"
)

func ExampleLooper() {
	ctx := context.Background()
	err := plumber.Start(ctx,
		plumber.Looper(func(ctx context.Context, l *plumber.Loop) error {
			return l.Run(func(done plumber.DoneFunc) {
				tick := time.Tick(500 * time.Millisecond)
				fmt.Println("Looper starting up")
				done.Done(func() error {
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
				})
			})
		}),
		plumber.TTL(600*time.Millisecond),
		plumber.CloseTimeout(2*time.Second),
	)
	if err != nil {
		fmt.Println("error: ", err)
	}
	// Output:
	// Looper starting up
	// Looper work
	// Looper requested to shutdown
	// Looper finished
}
