package plumber_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/getoutreach/plumber"
	"gotest.tools/v3/assert"
)

type Config struct {
	helloMessage plumber.D[string]
}

func (c *Config) HelloMessage() *plumber.D[string] {
	return c.helloMessage.Const("Hello message")
}

type HTTP struct {
	Server       plumber.R[*http.Server]
	HelloHandler plumber.D[http.HandlerFunc]
	EchoHandler  plumber.D[http.HandlerFunc]
}

type App struct {
	Config *Config
	HTTP   *HTTP
	D1     plumber.D[int]
	D2     plumber.D[string]
	d3     plumber.D[int]
	D4     plumber.R[int]
}

func (a *App) D3() *plumber.D[int] {
	return a.d3.Resolver(func(r *plumber.Resolution[int]) {
		r.Require(&a.D2, a.D3()).Then(func() {
			r.Resolve(0)
		})
	})
}

func TestDefineOnce(t *testing.T) {
	type dep struct {
		D1 int
		D2 int
	}
	a := struct {
		D1       plumber.D[int]
		D2       plumber.D[int]
		Resolved plumber.D[*dep]
	}{}
	a.D1.Const(1)
	a.D1.Const(2)
	a.D2.Define(func() int { return 3 })
	a.D2.Define(func() int { return 4 })

	assert.Equal(t, a.D1.Must(), 1)
	assert.Equal(t, a.D2.Must(), 3)
}

func TestConcurrency(t *testing.T) {
	for i := 0; i < 1000; i++ {
		t.Run("run", func(t *testing.T) {
			a := struct {
				D1 plumber.D[*http.Server]
				D2 plumber.D[*http.Server]
				D3 plumber.D[*http.Server]
			}{}

			a.D1.Named("D1").Define(func() *http.Server {
				return &http.Server{}
			})

			a.D2.Named("D2").Resolver(func(r *plumber.Resolution[*http.Server]) {
				r.Require(&a.D1).Then(func() {
					r.Resolve(&http.Server{})
				})
			})

			a.D3.Named("D3").Resolver(func(r *plumber.Resolution[*http.Server]) {
				r.Require(&a.D2).Then(func() {
					r.Resolve(&http.Server{})
				})
			})

			wg := sync.WaitGroup{}
			wg.Add(3)

			go func() {
				defer wg.Done()
				err := a.D3.Error()
				assert.NilError(t, err)
			}()

			go func() {
				defer wg.Done()
				err := a.D2.Error()
				assert.NilError(t, err)
			}()

			go func() {
				defer wg.Done()
				err := a.D1.Error()
				assert.NilError(t, err)
			}()

			wg.Wait()
		})
	}
}

func TestRequireOk(t *testing.T) {
	type dep struct {
		D1 int
		D2 int
	}
	a := struct {
		D1      plumber.D[int]
		D2      plumber.D[int]
		Service plumber.D[*dep]
	}{}
	a.D1.Const(1)
	a.D2.Const(2)
	a.Service.Resolver(func(r *plumber.Resolution[*dep]) {
		r.Require(&a.D1, &a.D2).Then(func() {
			r.Resolve(&dep{
				D1: a.D1.Instance(),
				D2: a.D2.Instance(),
			})
		})
	})
	v, err := a.Service.InstanceError()
	assert.NilError(t, err)
	assert.Equal(t, v.D1, 1)
	assert.Equal(t, v.D2, 2)
}

func TestRequireNotOk(t *testing.T) {
	type notresolved struct{}
	type middle struct{}
	a := struct {
		D1          plumber.D[int]
		D2          plumber.D[int]
		NotResolved plumber.D[*notresolved]
		Middle      plumber.D[middle]
	}{}
	a.D1.Const(1)
	a.Middle.Resolver(func(r *plumber.Resolution[middle]) {
		r.Require(&a.NotResolved).Then(func() {
			r.Resolve(middle{})
		})
	})
	a.D2.Resolver(func(r *plumber.Resolution[int]) {
		r.Require(&a.D1, &a.Middle).Then(func() {
			r.Resolve(1)
		})
	})
	v, err := a.D2.InstanceError()
	assert.Equal(t, v, 0)
	//nolint: lll //Why: error is long
	assert.Error(t, err, `dependency not resolved, int requires plumber_test.middle (dependency not resolved, plumber_test.middle requires *plumber_test.notresolved (instance *plumber_test.notresolved not resolved))`)
}

func TestRequireNotOkError(t *testing.T) {
	type notresolved struct{}
	type middle struct{}
	a := struct {
		D2        plumber.D[int]
		WithError plumber.D[*notresolved]
		Middle    plumber.D[middle]
	}{}

	a.WithError.Resolver(func(r *plumber.Resolution[*notresolved]) {
		r.Require().Then(func() {
			r.ResolveError(nil, errors.New("Error"))
		})
	})
	a.Middle.Resolver(func(r *plumber.Resolution[middle]) {
		r.Require(&a.WithError).Then(func() {
			r.Resolve(middle{})
		})
	})
	a.D2.Resolver(func(r *plumber.Resolution[int]) {
		r.Require(&a.Middle).Then(func() {
			r.Resolve(1)
		})
	})
	v, err := a.D2.InstanceError()
	assert.Equal(t, v, 0)
	//nolint: lll //Why: error is long
	assert.Error(t, err, `dependency not resolved, int requires plumber_test.middle (dependency not resolved, plumber_test.middle requires *plumber_test.notresolved (Error))`)
}

func TestRequireNotOkCycle(t *testing.T) {
	a := struct {
		D1 plumber.D[int]
		D2 plumber.D[int]
	}{}
	a.D1.Const(1)
	a.D2.Resolver(func(r *plumber.Resolution[int]) {
		r.Require(&a.D1, &a.D2).Then(func() {
			r.Resolve(1)
		})
	})
	v, err := a.D2.InstanceError()
	assert.Equal(t, v, 0)
	assert.Error(t, err, "dependency not resolved, int requires int (circular dependency)")
}

func TestRequireConcurrentOk(t *testing.T) {
	type concurrent struct{}
	a := struct {
		D1         plumber.D[int]
		D2         plumber.D[int]
		Concurrent plumber.D[concurrent]
	}{}
	a.D1.Const(1)
	a.D2.Resolver(func(r *plumber.Resolution[int]) {
		r.Require(&a.D1, &a.Concurrent).Then(func() {
			r.Resolve(1)
		})
	})
	a.Concurrent.Define(func() concurrent {
		time.Sleep(100 * time.Millisecond)
		return concurrent{}
	})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		v, err := a.D2.InstanceError()
		assert.NilError(t, err)
		assert.Equal(t, v, 1)
	}()
	go func() {
		defer wg.Done()
		_, err := a.Concurrent.InstanceError()
		assert.NilError(t, err)
	}()
	wg.Wait()
}

func TestExamplePipeline(t *testing.T) {
	a := &App{
		Config: &Config{},
		HTTP:   &HTTP{},
	}
	a.Config.helloMessage.Const("Message")
	fitHTTP(a)

	a.D1.Const(1)

	a.D2.Const("a")

	a.D2.DefineError(func() (string, error) {
		return "tesait", nil
	})

	a.D2.Define(func() string {
		return "tesait"
	})

	a.D4.Resolver(func(r *plumber.ResolutionR[int]) {
		r.Require(&a.D2).Then(func() {
			r.ResolveAdapter(0, plumber.Closer(func(context.Context) error {
				return nil
			}))
		})
	})

	fmt.Println("test", a.D1.Must(), a.D2.Must())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := plumber.Start(ctx,
		// Serial pipeline. Task are started sequentially and closed in reverse order.
		plumber.Pipeline(
			plumber.Closer(func(ctx context.Context) error {
				fmt.Println("pipeline is closing")
				return nil
			}),
			plumber.GracefulRunner(func(ctx context.Context) error {
				fmt.Println("Task 1 starting")
				<-ctx.Done()
				return nil
			}, func(ctx context.Context) error {
				fmt.Println("Task 1 closing")
				return nil
			}),
			// The parallel pipeline all task are stared and closed in parallel.
			plumber.Parallel(
				plumber.NewRunner(func(ctx context.Context) error {
					fmt.Println("Task 2 starting")
					<-ctx.Done()
					return nil
				}),
				plumber.NewRunner(func(ctx context.Context) error {
					fmt.Println("Task 3 starting")
					<-ctx.Done()
					return nil
				}),
				plumber.Looper(func(ctx context.Context, l *plumber.Loop) error {
					l.Ready()
					tick := time.Tick(500 * time.Millisecond)
					for {
						select {
						case <-tick:
							// Work
							fmt.Println("Work")
						case closeDone := <-l.Closing():
							fmt.Println("Close is requested")
							closeDone.Success()
							// Graceful shutdown
							return nil
						case <-ctx.Done():
							// Cancel / Timeout
							return ctx.Err()
						}
					}
				}),
			),
			// Dependency graph based runner
			&a.D4,
			&a.HTTP.Server,
		),
		// The pipeline needs to finish startup phase within 30 seconds. If not, run context is canceled. Close is initiated.
		plumber.ReadinessTimeout(2*time.Second),
		// The pipeline needs to gracefully close with 120 seconds. If not, internal run and close contexts are canceled.
		plumber.CloseTimeout(2*time.Second),
		// The pipeline will run for 120 seconds then will be closed gracefully.
		plumber.TTL(2*time.Second),
		// When given signals will be received pipeline will be closed gracefully.
		plumber.SignalCloser(),
	)

	if err != nil {
		fmt.Printf("Pipeline error: %v\n", err)
		return
	}
}

func fitHTTP(a *App) {
	a.HTTP.EchoHandler.Define(func() http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "back")
		}
	})
	a.HTTP.HelloHandler.Resolver(func(r *plumber.Resolution[http.HandlerFunc]) {
		r.Require(
			a.Config.HelloMessage(),
		).Then(func() {
			message := a.Config.HelloMessage().Instance()
			r.Resolve(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, "message:"+message)
			})
		})
	})
	a.HTTP.Server.Resolver(func(r *plumber.ResolutionR[*http.Server]) {
		r.Require(
			&a.HTTP.HelloHandler,
			&a.HTTP.EchoHandler,
		).Then(func() {
			httpServer := &http.Server{
				Addr: ":9090",
			}
			http.HandleFunc("/hello", a.HTTP.HelloHandler.Instance())
			http.HandleFunc("/echo", a.HTTP.EchoHandler.Instance())

			r.ResolveAdapter(httpServer, plumber.GracefulRunner(func(ctx context.Context) error {
				// ready is async to give time to server start
				fmt.Println("HTTP server is starting")
				if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
					err = fmt.Errorf("HTTP server ListenAndServe Error: %w", err)
					return err
				}
				fmt.Println("HTTP server is closed")
				return nil
			}, func(ctx context.Context) error {
				if err := httpServer.Shutdown(ctx); err != nil {
					return fmt.Errorf("HTTP Server Shutdown Error: %w", err)
				}
				fmt.Println("Closed HTTP server")
				return nil
			}))
		})
	})
}
