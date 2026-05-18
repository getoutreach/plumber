# Dependency Injection

Plumber provides a declarative, type-safe dependency management system for Go. Dependencies
are declared as struct fields, wired via resolvers, and resolved lazily on first access.
Errors propagate automatically through the graph.

## Core types

### `D[T]` — value dependency

The primary wrapper. Holds a lazily-resolved value of type `T`.

```go
var port plumber.D[int]
var repo plumber.D[*Repository]
```

### `R[T]` — runnable dependency

Like `D[T]`, but the resolved value must implement `Runner`. Can be placed directly into
`Pipeline()` or `Parallel()` blocks for lifecycle management.

```go
var server plumber.R[*http.Server]
```

`R[T]` implements `Runner`, `Closeable`, and `Readier` — delegating to the resolved value.

### `Resolution[T]` / `ResolutionR[T]`

Passed to `Resolver` callbacks. Provide:

| Method | Description |
|---|---|
| `Require(deps ...Dependency)` | Declare upstream dependencies; returns a `Future` |
| `Resolve(v)` | Set the resolved value |
| `Error(err)` | Set an error |
| `ResolveError(v, err)` | Set both value and error |

`ResolutionR[T]` adds `ResolveAdapter(v T, runnable Runner)` for cases where the value
and the runner are different objects.

### `Future`

Returned by `Require()`. Call `.Then(callback)` to execute logic only after all required
dependencies resolve successfully. If any dependency fails, the callback is skipped and the
error propagates.

## Declaring dependencies

### `Const` — static value, no dependencies, no error

```go
a.Port.Const(5000)
```

### `Define` — lazy factory, no dependencies

```go
a.Repository.Define(func() *Repository {
    return &Repository{}
})
```

### `DefineError` — lazy factory with error

```go
a.Repository.DefineError(func() (*Repository, error) {
    return database.NewRepository()
})
```

### `Resolver` — full dependency graph wiring

Use when the dependency requires other dependencies. Declare upstream deps with
`Require()`, resolve inside `Then()`.

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

**Always call `Instance()` inside `Then()`, never outside it.** The `Then` callback only
runs after all required dependencies resolve successfully.

### `Named` — human-readable name for error messages

```go
a.Port.Named("HTTPPort").Const(8080)
// Errors read: "HTTPPort(int) not resolved" instead of "int not resolved"
```

## Accessing values

| Method | Behavior |
|---|---|
| `Instance()` | Returns value. Triggers lazy resolution if needed. |
| `InstanceError()` | Returns `(value, error)`. Safe for checking resolution errors. |
| `Must()` | Returns value or panics on error. |
| `MakeInstance()` | Re-invokes factory for a fresh instance (bypasses cache). |

## Container pattern

Group related dependencies into container structs. Each container has a `Define` method
that wires its dependencies.

### Sub-container struct

```go
type Database struct {
    Repository         plumber.D[Repository]
    BatchingRepository plumber.R[*BatchingRepository]
}

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

### Root container with `DefineContainers`

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

### Environment-specific overrides (definers)

Definers run **before** sub-container `Define()` methods. Combined with define-once
semantics, they override defaults:

```go
type Definer = func(ctx context.Context, cf *Config, a *Container)

func WithIntegrationEnvironment(ctx context.Context, cf *Config, a *Container) {
    a.Grpc.Port.Const(1000) // wins because it runs before Grpc.Define()
}

a := NewApplication(ctx, cfg, WithIntegrationEnvironment)
```

### Container validation with `ContainerResolved`

Validate the entire dependency graph in tests:

```go
func TestContainer(t *testing.T) {
    err := plumber.ContainerResolved(func() *Container {
        return NewApplication(context.Background(), &Config{})
    })
    assert.NilError(t, err)
}
```

Detects: undefined dependencies, transitive failures, declared-but-unused dependencies,
and circular references.

Use `plumber:",ignore"` struct tag to skip specific fields during validation:

```go
type Bugs struct {
    Server plumber.R[*Server] `plumber:",ignore"`
}
```

## Advanced features

### `Wrap` — decorator pattern

Register post-resolution wrappers that transform the value after resolution:

```go
a.Repository.Wrap(func(r Repository) Repository {
    return NewCachingRepository(r)
})
```

### `Use` — testing overrides

Directly set a value, bypassing normal definition. **For testing only.**

```go
a.Repository.Use(mockRepository)
```

### `WhenResolved` — post-resolution callbacks

```go
a.Connection.WhenResolved(func() {
    container.CleanupError(func(ctx context.Context) error {
        return a.Connection.Instance().Close()
    })
})
```

### `ResolveAdapter` — runnable dependencies

When the dependency value and its runner are different objects:

```go
a.HTTP.Server.Resolver(func(r *plumber.ResolutionR[*http.Server]) {
    r.Require(&a.Handler).Then(func() {
        server := &http.Server{Addr: ":8080"}
        r.ResolveAdapter(server, plumber.GracefulRunner(
            func(ctx context.Context) error { return server.ListenAndServe() },
            func(ctx context.Context) error { return server.Shutdown(ctx) },
        ))
    })
})
```

## Key resolution rules

1. **Lazy**: values resolve on first access, not at declaration time.
2. **Define-once**: only the first `Const`/`Define`/`DefineError`/`Resolver` call takes effect. Subsequent calls are silently ignored.
3. **Error propagation**: errors propagate transitively with full path descriptions.
4. **Circular detection**: `Require().Then()` walks the graph and fails immediately on cycles.
5. **Concurrency safe**: multiple goroutines can call `Instance()` simultaneously.

## Common mistakes

- **Calling `Instance()` outside `Then()`**: the dependency may not be resolved yet. Always access inside the `Then` callback.
- **Forgetting `Require()`**: if you use `a.Dep.Instance()` inside `Then()` but don't list `&a.Dep` in `Require()`, the dependency graph won't know about the relationship. `ContainerResolved` will flag this as "declared but not used".
- **Using `Use()` in production code**: `Use` bypasses the definition pipeline and is intended for tests only.
- **Expecting re-definition to override**: due to define-once semantics, the first definition wins. To override defaults, use the definer pattern (definers run before `Define()`).
