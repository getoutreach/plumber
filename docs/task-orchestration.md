# Service Task Orchestration

The application might contain several layers that might communicate with each other so there might be a need to start certain tasks in predefined order. Some other might be independent so those can be started and closed in parallel.

Also when doing graceful shutdown the tasks needs to be closed in reversed order so no layer is running without required dependencies.

## Example

```golang
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

signal := plumber.NewSignal()

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
            // Runner that implements Runner, Readier, Closeable
            plumber.NewRunner(
                func(ctx context.Context) error {
                    go func() {
                        time.Sleep(1 * time.Second)
                        fmt.Println("Task 2 is ready")
                        signal.Notify()
                    }()
                    fmt.Println("Task 2 starting")
                    <-ctx.Done()
                    return nil
                },
                plumber.WithClose(func(ctx context.Context) error {
                    fmt.Println("Task 2 closing")
                    return nil
                }),
                plumber.WithReady(signal),
            )
            plumber.NewRunner(func(ctx context.Context) error {
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
    plumber.Readiness(30*time.Second),
    // The pipeline needs to gracefully close with 120 seconds. If not, internal run and close contexts are canceled.
    plumber.CloseTimeout(120*time.Second),
    // The pipeline will run for 120 seconds then will be closed gracefully.
    plumber.TTL(120*time.Second),
    // When given signals will be received pipeline will be closed gracefully.
    plumber.SignalCloser(),
)
```
