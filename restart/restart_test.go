package restart

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"errors"

	"github.com/getoutreach/plumber"
	"gotest.tools/v3/assert"
)

func TestRestarter(t *testing.T) {
	var restarts uint32
	s := struct {
		Runner plumber.D[plumber.Runner]
	}{}

	s.Runner.Define(func() plumber.Runner {
		var closing bool
		return plumber.Looper(func(ctx context.Context, l *plumber.Loop) error {
			select {
			case closeDone := <-l.Closing():
				closeDone.Success()
				closing = true
			case <-ctx.Done():
				return ctx.Err()
			default:
				fmt.Println("Running ------------------------------", restarts)
				if closing {
					return nil
				}
				atomic.AddUint32(&restarts, 1)
				return errors.New("an error")
			}
			return nil
		})
	})

	runner := Restartable(&s.Runner, Always(), WithDelay(200*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := runner.Run(ctx)
		assert.ErrorContains(t, err, "an error")
	}()

	time.Sleep(1 * time.Second)

	err := plumber.RunnerClose(ctx, runner)
	assert.NilError(t, err)

	wg.Wait()

	assert.Assert(t, restarts >= 3, "restarts: %d", atomic.LoadUint32(&restarts))
}
