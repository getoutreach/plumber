---
name: or-plumber-orchestration
description: "Provides guidance on plumber task orchestration: starting, sequencing, and gracefully shutting down long-running services using Pipeline, Parallel, runners, and lifecycle options."
---

# Task Orchestration

Plumber provides lifecycle-managed task orchestration for long-running services. Tasks are
organized into serial pipelines and parallel groups, with automatic reverse-order shutdown,
readiness signaling, and configurable timeouts.

## Entry point

```go
err := plumber.Start(ctx, pipeline, ...options)
```

`Start` runs the pipeline, blocks until completion or shutdown, and returns any error.

## Pipeline combinators

### `Pipeline()` — serial execution

Tasks start sequentially (top to bottom). On shutdown, tasks close in **reverse order**
so no layer runs without its dependencies.

```go
plumber.Pipeline(
    &a.Database.BatchingRepository,  // starts first, closes last
    &a.Async.Publisher,              // starts second
    &a.Grpc.Server,                  // starts third, closes first
)
```

### `Parallel()` — concurrent execution

All tasks start and close concurrently.

```go
plumber.Parallel(
    &a.Grpc.Server,
    &a.Graphql.Server,
)
```

### Nesting

Combinators nest freely. A common pattern is serial infrastructure startup with parallel
application servers:

```go
plumber.Pipeline(
    &a.Database.BatchingRepository,
    &a.Async.Publisher,
    plumber.Parallel(
        &a.Grpc.Server,
        &a.Graphql.Server,
    ),
)
```

## Runner types

### `GracefulRunner` — start + stop functions

The simplest runner. Provide a run function and a close function. Often used to wrap existing server types that have separate `ListenAndServe` and `Shutdown` methods:

```go

server := &http.Server{Addr: ":8080"}

return plumber.GracefulRunner(
    func(ctx context.Context) error {
        // Run — block until ctx is done or work completes
        return server.ListenAndServe()
    },
    server.Shutdown,
)
```

### `NewRunner` — runner with options

More flexible. Supports optional close and readiness:

```go
signal := plumber.NewSignal()

plumber.NewRunner(
    func(ctx context.Context) error {
        go func() {
            // become ready asynchronously
            signal.Notify()
        }()
        <-ctx.Done()
        return nil
    },
    plumber.WithClose(func(ctx context.Context) error {
        return cleanup()
    }),
    plumber.WithReady(signal),
)
```

### `Looper` — periodic work with graceful shutdown

The most common runner that exposes all the lifecycle features. The run function receives a `Loop` that provides channels for shutdown signals and readiness notification. The run function should block until shutdown is requested or work is complete. This is the ideal runner for long-running services that need to do periodic work or maintain state.

```go
plumber.Looper(func(ctx context.Context, l *plumber.Loop) error {
    l.Ready()
    tick := time.Tick(500 * time.Millisecond)
    for {
        select {
        case <-tick:
            // do periodic work
        case closeDone := <-l.Closing():
            closeDone.Success()
            return nil
        case <-ctx.Done():
            return ctx.Err()
        }
    }
})
```

Key `Loop` methods:
- `l.Ready()` — signal readiness
- `l.Closing()` — channel that receives when graceful close is requested
- `closeDone.Success()` / `closeDone.Error(err)` — acknowledge close completion

### `Closer` — close-only task

Runs only during shutdown (no run phase):

```go
plumber.Closer(func(ctx context.Context) error {
    fmt.Println("pipeline is closing")
    return nil
})
```

### `R[T]` — DI-integrated runners

`R[T]` dependencies from the DI system implement `Runner` and can be placed directly into
pipelines:

```go
plumber.Pipeline(
    &a.Database.BatchingRepository,  // R[*BatchingRepository]
    &a.Async.Publisher,              // R[*async.Publisher]
    &a.Grpc.Server,                  // R[*grpc.Server]
)
```

## Readiness

Tasks signal readiness to indicate they are fully operational. The pipeline waits for each
task to become ready before starting the next one (in `Pipeline` mode) or before
considering the group ready (in `Parallel` mode).

Use `plumber.NewSignal()` for custom readiness:

```go
signal := plumber.NewSignal()
// ... in runner: signal.Notify()
// ... in options: plumber.WithReady(signal)
```

## Lifecycle options

Pass options to `plumber.Start()` to control timeouts and shutdown triggers.

| Option | Description |
|---|---|
| `plumber.Readiness(timeout)` | Pipeline must complete startup within this duration. If exceeded, context is canceled and close is initiated. |
| `plumber.CloseTimeout(timeout)` | Pipeline must gracefully close within this duration. If exceeded, internal contexts are canceled. |
| `plumber.TTL(duration)` | Pipeline runs for this duration then closes gracefully. |
| `plumber.SignalCloser()` | Close pipeline on OS signals (SIGINT, SIGTERM). |

```go
err := plumber.Start(ctx,
    plumber.Pipeline(
        &a.Database.BatchingRepository,
        &a.Async.Publisher,
        plumber.Parallel(
            &a.Grpc.Server,
            &a.Graphql.Server,
        ),
    ),
    plumber.Readiness(30*time.Second),
    plumber.CloseTimeout(120*time.Second),
    plumber.TTL(120*time.Second),
    plumber.SignalCloser(),
)
```

## Key rules

1. **Runners must block.** A runner's run function should block until `ctx.Done()`, a closing signal, or work completion. Returning immediately means the task is "done" and the pipeline proceeds.
2. **Close order is reverse of start.** In a `Pipeline`, the last task to start is the first to close. This ensures dependencies outlive their dependents.
3. **Readiness gates the next stage.** In a `Pipeline`, each task must signal readiness before the next task starts. If a task never becomes ready and `Readiness` timeout is set, the pipeline fails.
4. **Errors propagate.** If any task's run function returns a non-nil error, the pipeline initiates shutdown.
