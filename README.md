# plumber
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/getoutreach/plumber)
[![Generated via Bootstrap](https://img.shields.io/badge/Outreach-Bootstrap-%235951ff)](https://github.com/getoutreach/bootstrap)
[![Coverage Status](https://coveralls.io/repos/github/getoutreach/plumber/badge.svg?branch=)](https://coveralls.io/github//getoutreach/plumber?branch=)

A library to manage application dependency graph and orchestrate service tasks.

<img src="plumber.png" width="300"/>

## Contributing

Please read the [CONTRIBUTING.md](CONTRIBUTING.md) document for guidelines on developing and contributing changes.

## High-level Overview

### Service dependency management

Simple but effective dependency management that is focused on readability. Main goal is to get rid off the repetitive error checking during the construction.

instead of following sequence:

```golang
d1, err := buildD1()
if err != nil {
    return nil, fmt.Errorf("d1 failed: %w", err)
}

d2, err := buildD2()
if err != nil {
    return nil, fmt.Errorf("d1 failed: %w", err)
}

return &Service{
	D1: d1,
	D2: d2,
}, nil

```

We do just:

```golang
a.Service.Resolve(func(r *plumber.Resolution[*Service]) {
    r.Require(&a.D1, &a.D2).Then(func() {
        r.Resolve(&Service{
            D1: a.D1.Instance(),
            D2: a.D2.Instance(),
        })
    })
})
```

This example contains just two dependencies with in real world it might gets much more messier. Also dependencies are build just once and then reused.

#### Example
```golang
// a service we want to build with certain dependencies
type service struct {
    D1 int
    D2 int
}
// application dependency graph holds all application dependencies. It is recommended to structure it based on adapter types.
a := struct {
    D1      plumber.D[int]
    D2      plumber.D[int]
    Service plumber.D[*service]
}{}
// definition of the dependencies
a.D1.Const(1)
a.D2.Const(2)

// service resolver
a.Service.Resolve(func(r *plumber.Resolution[*Service]) {
    // service depends on D1 and D2 those needs gets resolved first without an error.
    r.Require(&a.D1, &a.D2).Then(func() {
        // When all good, we can construct our service
        r.Resolve(&Service{
            D1: a.D1.Instance(),
            D2: a.D2.Instance(),
        })
    })
})
v, err := a.Service.InstanceError()
assert.NilError(t, err)
assert.Equal(t, v.D1, 1)
assert.Equal(t, v.D2, 2)
```

### Service task orchestration

The application might contain several layers that might communicate with each other so there might be a need to start certain tasks in predefined order. Some other might be independent so those can be started and closed in parallel.

Also when doing graceful shutdown the tasks needs to be closed in reversed order so no layer is running without required dependencies.

```golang
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

signaler := plumber.NewErrorSignaler()

err := plumber.Start(ctx,
    // Serial pipeline. Task are started sequentially and closed in reverse order.
    plumber.Pipeline(
        plumber.Closer(func(ctx context.Context) error {
            fmt.Println("pipeline is closing")
            return nil
        }),
        plumber.GracefulRunner(func(ctx context.Context, ready plumber.ReadyFunc) error {
            ready()
            fmt.Println("Task 1 starting")
            <-ctx.Done()
            return nil
        }, func(ctx context.Context) error {
            fmt.Println("Task 1 closing")
            return nil
        }),
        // The parallel pipeline all task are stared and closed in parallel.
        plumber.Parallel(
            plumber.SimpleRunner(func(ctx context.Context) error {
                fmt.Println("Task 2 starting")
                <-ctx.Done()
                return nil
            }),
            plumber.SimpleRunner(func(ctx context.Context) error {
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
    ).With(plumber.Signaler(signaler)),
    // The pipeline needs to finish startup phase within 30 seconds. If not, run context is canceled. Close is initiated.
    plumber.Readiness(30*time.Second),
    // The pipeline needs to gracefully close with 120 seconds. If not, internal run and close contexts are canceled.
    plumber.CloseTimeout(120*time.Second),
    // The pipeline will run for 120 seconds then will be closed gracefully.
    plumber.TTL(120*time.Second),
    // When given signals will be received pipeline will be closed gracefully.
    plumber.SignalCloser(),
    // When some tasks covered with signaler reports and error pipeline will be closed.
    plumber.Closing(signaler),
)
```
