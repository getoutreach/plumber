# Service Dependency Management

Plumber provides a declarative, type-safe dependency management system for Go applications.
The main goal is to eliminate repetitive error checking during construction and make
dependency graphs readable, validated, and testable.

## Overview

Instead of manually wiring dependencies with error checks at every step:

```go
d1, err := buildD1()
if err != nil {
    return nil, fmt.Errorf("d1 failed: %w", err)
}

d2, err := buildD2()
if err != nil {
    return nil, fmt.Errorf("d2 failed: %w", err)
}

return &Service{D1: d1, D2: d2}, nil
```

You declare your graph declaratively:

```go
a.Service.Resolver(func(r *plumber.Resolution[*Service]) {
    r.Require(&a.D1, &a.D2).Then(func() {
        r.Resolve(&Service{
            D1: a.D1.Instance(),
            D2: a.D2.Instance(),
        })
    })
})
```

Dependencies are built once and reused. Errors propagate automatically through the graph.

---

## Core types

### `D[T]` — Value dependency

The primary dependency wrapper. Holds a lazily-resolved value of type `T`.

```go
var port plumber.D[int]
var repo plumber.D[*Repository]
```

### `R[T]` — Runnable dependency

Like `D[T]`, but the resolved value must implement `Runner`. This allows `R[T]` to
be placed directly into a `Pipeline()` or `Parallel()` block for lifecycle management.

```go
var server plumber.R[*http.Server]
var publisher plumber.R[*async.Publisher]
```

`R[T]` implements `Runner`, `Closeable`, and `Readier` — delegating to the resolved value.

### `Resolution[T]` and `ResolutionR[T]`

Orchestrators passed to `Resolver` callbacks. They provide:

| Method | Description |
|---|---|
| `Require(deps ...Dependency)` | Declare upstream dependencies; returns a `Future` |
| `Resolve(v)` | Set the resolved value |
| `Error(err)` | Set an error |
| `ResolveError(v, err)` | Set both value and error |

`ResolutionR[T]` adds `ResolveAdapter(v T, runnable Runner)` for cases where the
value type and the runner are different objects (see [ResolveAdapter](#rt-and-resolveadapter--runnable-dependencies-in-pipelines)).

### `Future[T]`

Returned by `Require()`. Call `.Then(callback)` to execute logic only after all
required dependencies resolve successfully. If any dependency fails, the callback is
skipped and the error propagates.

---

## Declaring dependencies

### `Const` — static value

Assign a value directly. No dependencies, no error handling.

```go
a.Port.Const(5000)
a.Name.Const("my-service")
```

### `Define` — lazy factory

Provide a factory function. Called lazily on first access.

```go
a.Repository.Define(func() *Repository {
    return &Repository{}
})
```

### `DefineError` — lazy factory with error

Like `Define`, but the factory can return an error.

```go
a.Repository.DefineError(func() (*Repository, error) {
    return database.NewRepository()
})
```

### `Resolver` — full dependency graph wiring

The most powerful form. Declare upstream dependencies with `Require()`, then resolve
inside `Then()` — which only runs if all requirements are satisfied.

```go
a.Service.Resolver(func(r *plumber.Resolution[*Service]) {
    r.Require(&a.Repository, &a.Publisher).Then(func() {
        r.Resolve(&Service{
            Repo: a.Repository.Instance(),
            Pub:  a.Publisher.Instance(),
        })
    })
})
```

Multiple dependencies can be required at once. The graph resolves transitively —
if `Repository` itself requires `DatabaseConnection`, that will be resolved first.

### `Named` — better error messages

Give a dependency a human-readable name that appears in error messages.

```go
a.Port.Named("HTTPPort").Const(8080)

// Or using the constructor:
port := plumber.Named[int]("HTTPPort")
```

Error messages then read: `"HTTPPort(int) not resolved"` instead of `"int not resolved"`.

---

## Accessing values

### `Instance` / `InstanceError` / `Must`

| Method | Behavior |
|---|---|
| `Instance()` | Returns the value. Triggers lazy resolution if needed. |
| `InstanceError()` | Returns `(value, error)`. Safe way to check for resolution errors. |
| `Must()` | Returns the value or panics on error. |

```go
v, err := a.Service.InstanceError()
// or
v := a.Service.Must()
```

### `MakeInstance` / `MakeInstanceError` — fresh instances

Re-invokes the factory to create a **new** instance, bypassing the cache. The cached
singleton from `Instance()` remains unchanged.

```go
a.Counter.Define(func() int {
    return generateID()
})

first := a.Counter.Instance()       // cached — always returns the same value
fresh := a.Counter.MakeInstance()    // new invocation — different value
same  := a.Counter.Instance()       // still returns `first`
```

---

## Resolution mechanics

### Lazy resolution

Values are not resolved at declaration time. Resolution is triggered on first access
(`Instance()`, `InstanceError()`, `Must()`, or `Error()`). This means the order of
`Const` / `Define` / `Resolver` calls does not matter — only the access order does.

### Define-once semantics

Only the **first** call to `Const`, `Define`, `DefineError`, or `Resolver` takes effect.
Subsequent definition calls on the same dependency are silently ignored.

```go
a.Port.Const(8080)
a.Port.Const(9090)  // ignored — a.Port.Instance() returns 8080
```

This enables a pattern where sub-containers provide defaults that can be overridden
by defining the dependency first (see [Environment-specific overrides](#environment-specific-overrides-definers)).

### Error propagation

Errors propagate transitively through the dependency graph with a descriptive chain:

```go
a.Middle.Resolver(func(r *plumber.Resolution[Middle]) {
    r.Require(&a.Broken).Then(func() {
        r.Resolve(Middle{})
    })
})

// a.Middle.Error() returns:
// "dependency not resolved, Middle requires *Broken (instance *Broken not resolved)"
```

If a deep dependency fails, every dependent in the chain reports the full error path.

### Circular dependency detection

`Future.Then()` walks each dependency's graph before resolving. If the current
dependency appears in any upstream graph, resolution fails immediately:

```go
a.D1.Resolver(func(r *plumber.Resolution[int]) {
    r.Require(&a.D1).Then(func() { // self-reference
        r.Resolve(1)
    })
})
// Error: "circular dependency"
```

### Concurrency safety

`D[T]` and `R[T]` are safe for concurrent access. Multiple goroutines can call
`Instance()` simultaneously — only one will trigger resolution, others will wait.

---

## Advanced features

### `Wrap` — decorator pattern

Register post-resolution wrappers that transform the value after it is resolved.
Wrappers run in registration order.

```go
a.Repository.DefineError(func() (Repository, error) {
    return database.NewRepository()
})

// In test setup — wrap with caching layer:
a.Repository.Wrap(func(r Repository) Repository {
    return NewCachingRepository(r)
})
```

### `Use` — testing overrides

Directly set a value, bypassing the normal definition. Intended **for testing only**.
Unlike `Const`, this works even if the dependency was already defined.

```go
a.Repository.Use(mockRepository)
```

### `WhenResolved` — post-resolution callbacks

Register a callback that fires after a dependency resolves. Useful for side effects
like registering cleanup functions.

```go
a.Connection.WhenResolved(func() {
    container.CleanupError(func(ctx context.Context) error {
        return a.Connection.Instance().Close()
    })
})
```

### `R[T]` and `ResolveAdapter` — runnable dependencies in pipelines

`R[T]` resolves to a value that can participate in plumber's task orchestration.
Use `ResolveAdapter` when the dependency value and its runner are different objects:

```go
a.HTTP.Server.Resolver(func(r *plumber.ResolutionR[*http.Server]) {
    r.Require(&a.Handler).Then(func() {
        server := &http.Server{Addr: ":8080"}
        http.HandleFunc("/", a.Handler.Instance())

        r.ResolveAdapter(server, plumber.GracefulRunner(
            func(ctx context.Context) error {
                return server.ListenAndServe()
            },
            func(ctx context.Context) error {
                return server.Shutdown(ctx)
            },
        ))
    })
})

// R[T] can be placed directly in a pipeline:
plumber.Start(ctx, plumber.Pipeline(
    &a.HTTP.Server,
))
```

The value (`*http.Server`) is accessible via `a.HTTP.Server.Instance()`, while the
runner/closer behavior is handled by the `GracefulRunner` adapter.

---

## Container pattern

### Structuring containers

Group related dependencies into container structs. Each container holds `D[T]` and
`R[T]` fields representing its domain's dependencies.

```go
type Database struct {
    Repository         plumber.D[Repository]
    BatchingRepository plumber.R[*BatchingRepository]
}

type Grpc struct {
    Port   plumber.D[int32]
    Server plumber.R[*grpc.Server]
}
```

### Sub-containers with `Define()`

Each container implements a `Define` method that wires its dependencies, receiving
the config and root container for cross-container references:

```go
func (c *Database) Define(ctx context.Context, cf *Config, a *Container) {
    c.Repository.DefineError(func() (Repository, error) {
        return database.NewRepository()
    })

    c.BatchingRepository.Resolver(func(r *plumber.ResolutionR[*BatchingRepository]) {
        r.Require(&c.Repository).Then(func() {
            r.ResolveError(database.NewBatchingRepository(c.Repository.Instance(), 100))
        })
    })
}
```

### `DefineContainers` — composition

Compose the root container from sub-containers in a single call:

```go
type Container struct {
    plumber.Container
    Database *Database
    Grpc     *Grpc
    Service  *Service
}

func NewApplication(ctx context.Context, cf *Config, definers ...Definer) *Container {
    a := &Container{
        Database: new(Database),
        Grpc:     new(Grpc),
        Service:  new(Service),
    }
    return plumber.DefineContainers(ctx, cf, definers, a,
        a.Database, a.Grpc, a.Service,
    )
}
```

`DefineContainers` calls each definer function first, then calls `Define()` on each
sub-container in order.

### Container cleanup

Embed `plumber.Container` to get cleanup lifecycle support:

```go
type Container struct {
    plumber.Container
    // ...
}

// Register cleanup during resolution:
a.CleanupError(func(ctx context.Context) error {
    return a.Database.Connection.Instance().Close()
})

// On shutdown:
err := a.Close(ctx)
```

### `ContainerResolved` — graph validation

Validate the entire dependency graph in one call. This checks that every dependency
is defined, resolves without error, has no circular references, and that all declared
requirements are actually used.

```go
func TestContainer(t *testing.T) {
    err := plumber.ContainerResolved(func() *Container {
        return NewApplication(context.Background(), &Config{})
    })
    assert.NilError(t, err)
}
```

Detected problems include:
- **Undefined dependencies**: `instance *Publisher not resolved`
- **Transitive failures**: `dependency not resolved, Server requires *Publisher (...)`
- **Declared but unused**: `dependency declared but not used: Port(int32)` — when a
  dependency is listed in `Require()` but its `Instance()` is never called in `Then()`

### `plumber:",ignore"` struct tag

Skip specific fields during `ContainerResolved` validation. Useful for intentionally
broken or optional dependencies:

```go
type Bugs struct {
    Server plumber.R[*Server] `plumber:",ignore"`
}
```

### Environment-specific overrides (Definers)

Definers are functions that run **before** sub-container `Define()` methods. Combined
with define-once semantics, this lets you override defaults per environment:

```go
type Definer = func(ctx context.Context, cf *Config, a *Container)

// Override the default port for integration tests:
func WithIntegrationEnvironment(ctx context.Context, cf *Config, a *Container) {
    a.Grpc.Port.Const(1000)  // wins because it runs before Grpc.Define()
}

a := NewApplication(ctx, cfg, WithIntegrationEnvironment)
// a.Grpc.Port.Instance() == 1000 (not 5000, which is the default in Grpc.Define)
```

### `Resolved` helper

Check that a set of dependencies all resolved without error:

```go
err := plumber.Resolved(
    &a.Database.BatchingRepository,
    &a.Async.Publisher,
    &a.Grpc.Server,
)
```

---

## Full example

A complete application wiring database, async, gRPC, and GraphQL layers:

```go
func main() {
    a := example.NewApplication(context.Background(), &example.Config{
        AsyncBroker: "broker.service:9092",
    })

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    err := plumber.Start(ctx,
        plumber.Pipeline(
            &a.Database.BatchingRepository,
            &a.Async.Publisher,
            plumber.Parallel(
                &a.Grpc.Server,
                &a.Graphql.Server,
            ),
        ),
        plumber.TTL(5*time.Second),
        plumber.ReadinessTimeout(1*time.Second),
        plumber.CloseTimeout(5*time.Second),
        plumber.SignalCloser(),
    )
    if err != nil {
        fmt.Println("err:", err)
    }
}
```

Here, `R[T]` dependencies (`BatchingRepository`, `Publisher`, `Server`) participate
directly in the pipeline. The database and async layers start first (serial pipeline),
then gRPC and GraphQL start in parallel. On shutdown, the order reverses.

See the [`example/`](../example/) directory for the full source.
