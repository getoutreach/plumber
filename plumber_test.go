package plumber_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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
	return a.d3.Resolve(func(r *plumber.Resolution[int]) {
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
	a.Service.Resolve(func(r *plumber.Resolution[*dep]) {
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
	type dep struct{}
	a := struct {
		D1          plumber.D[int]
		D2          plumber.D[int]
		NotResolved plumber.D[*dep]
	}{}
	a.D1.Const(1)
	a.D2.Resolve(func(r *plumber.Resolution[int]) {
		r.Require(&a.D1, &a.NotResolved).Then(func() {
			r.Resolve(1)
		})
	})
	v, err := a.D2.InstanceError()
	assert.Equal(t, v, 0)
	assert.Error(t, err, "dependency not resolved, int requires *plumber_test.dep")
}

func TestRequireNotOkCycle(t *testing.T) {
	a := struct {
		D1 plumber.D[int]
		D2 plumber.D[int]
	}{}
	a.D1.Const(1)
	a.D2.Resolve(func(r *plumber.Resolution[int]) {
		r.Require(&a.D1, &a.D2).Then(func() {
			r.Resolve(1)
		})
	})
	v, err := a.D2.InstanceError()
	assert.Equal(t, v, 0)
	assert.Error(t, err, "dependency not resolved, int requires int")
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

	a.D4.Resolve(func(r *plumber.ResolutionR[int]) {
		r.Require(&a.D2).Then(func() {
			r.ResolveAdapter(0, plumber.Closer(func(context.Context) error {
				return nil
			}))
		})
	})

	fmt.Println("test", a.D1.Must(), a.D2.Must())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signaler := plumber.NewErrorSignaler()

	err := plumber.Start(ctx,
		plumber.Pipeline(
			plumber.Closer(func(ctx context.Context) error {
				fmt.Println("pipeline is closing")
				return nil
			}),
			plumber.GracefulRunner(func(ctx context.Context, done plumber.DoneFunc) error {
				defer done.Success()
				fmt.Println("Task 1 starting")
				return nil
			}, func(ctx context.Context) error {
				fmt.Println("Task 1 closing")
				return nil
			}),
			plumber.Parallel(
				plumber.Runner(func(ctx context.Context, done plumber.DoneFunc) error {
					defer done.Success()
					fmt.Println("Task 2 starting")
					return nil
				}),
				plumber.Runner(func(ctx context.Context, done plumber.DoneFunc) error {
					defer done.Success()
					fmt.Println("Task 3 starting")
					return nil
				}),
				plumber.Looper(func(ctx context.Context, l *plumber.Loop) error {
					return l.Run(func(done plumber.DoneFunc) {
						tick := time.Tick(500 * time.Millisecond)
						done.Done(func() error {
							for {
								select {
								case <-tick:
									// Work
									fmt.Println("Work")
								case closeDone := <-l.Closing():
									closeDone.Success()
									fmt.Println("Lopper closing")
									// Graceful shutdown
									return nil
								case <-ctx.Done():
									// Cancel / Timeout
									return ctx.Err()
								}
							}
						})
					})
				}),
			),
			&a.D4,
			&a.HTTP.Server,
		).With(plumber.Signaler(signaler)),
		plumber.Readiness(30*time.Second),
		plumber.TTL(2*time.Second),
		plumber.CloseTimeout(120*time.Second),
		plumber.SignalCloser(),
		plumber.CloserFunc(func(close func()) {

		}),
		plumber.Closing(signaler),
		plumber.ContextCloser(ctx),
	)

	if err != nil {
		fmt.Printf("Pipeline error: %v\n", err)
		return
	}
}

func fitHTTP(a *App) {
	a.HTTP.EchoHandler.Define(func() http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "back")
		}
	})
	a.HTTP.HelloHandler.Resolve(func(r *plumber.Resolution[http.HandlerFunc]) {
		r.Require(
			a.Config.HelloMessage(),
		).Then(func() {
			message := a.Config.HelloMessage().Instance()
			r.Resolve(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, "message: "+message)
			})
		})
	})
	a.HTTP.Server.Resolve(func(r *plumber.ResolutionR[*http.Server]) {
		r.Require(
			&a.HTTP.HelloHandler,
			&a.HTTP.EchoHandler,
		).Then(func() {
			httpServer := &http.Server{
				Addr: ":9090",
			}
			http.HandleFunc("/hello", a.HTTP.HelloHandler.Instance())
			http.HandleFunc("/echo", a.HTTP.EchoHandler.Instance())

			r.ResolveAdapter(httpServer, plumber.GracefulRunner(func(ctx context.Context, done plumber.DoneFunc) error {
				go func() {
					done.Done(func() error {
						fmt.Println("HTTP server is starting")
						if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
							err = fmt.Errorf("HTTP server ListenAndServe Error: %w", err)
							return err
						}
						fmt.Println("HTTP server is closed")
						return nil
					})
				}()
				return nil
			}, func(ctx context.Context) error {
				fmt.Println("Closing HTTP server")
				if err := httpServer.Shutdown(ctx); err != nil {
					return fmt.Errorf("HTTP Server Shutdown Error: %w", err)
				}
				return nil
			}))
		})
	})
}
