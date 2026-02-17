# Plumber Codebase Instructions

## Project Overview
Plumber is a Go library for managing application dependency graphs and orchestrating service tasks. It provides two core capabilities:
1. **Dependency resolution** via `D[T]` and `R[T]` wrappers
2. **Task orchestration** via Pipeline/Parallel runners with lifecycle management

## Core Architecture Patterns

### Dependency Resolution Pattern
Dependencies are wrapped in `D[T]` (basic) or `R[T]` (runnable) types and resolved using `Resolver()`:

```go
// Define dependencies
type Container struct {
    Port   plumber.D[int32]
    Server plumber.R[*grpc.Server]
}

// Resolve with explicit requirements
c.Server.Resolver(func(r *plumber.ResolutionR[*grpc.Server]) {
    r.Require(&a.Service.Querier, &a.Service.Notifier).Then(func() {
        r.ResolveError(grpc.NewServer(...))
    })
})
```

**Key rules:**
- Use `Const()` for constant values, `Define()` for computed values, `Resolver()` for dependencies with requirements
- Always call `Require()` before `Then()` to establish dependency graph
- Access resolved values via `Instance()` or `Must()` (panics on error)
- Check circular dependencies - they cause `ErrCircularDependency`

### Container Pattern
Organize dependencies by adapter/service type in nested structs. See [example/application.go](example/application.go):

```go
type Container struct {
    plumber.Container  // Embeds cleanup support
    Async    *Async
    Database *Database
    GRPC     *GRPC
}
```

Each sub-container implements `Define(ctx, config, root)` to resolve its dependencies. Use `DefineContainers()` to wire everything up.

### Task Orchestration Patterns

#### Pipeline (Serial)
Tasks start sequentially and close in reverse order:
```go
plumber.Pipeline(
    &a.Database.BatchingRepository,  // Starts first, closes last
    &a.Async.Publisher,
    plumber.Parallel(...),            // Can nest parallel inside serial
)
```

#### Parallel
Tasks start/close concurrently:
```go
plumber.Parallel(
    &a.GRPC.Server,
    &a.GraphQL.Server,
)
```

**Start() Options:**
- `ReadinessTimeout(d)` - Max time for all runners to signal Ready()
- `CloseTimeout(d)` - Max time for graceful shutdown
- `TTL(d)` - Auto-close after duration
- `SignalCloser()` - Close on OS signals

### Runner Interface Hierarchy
Implement these interfaces for orchestration compatibility:

1. **Runner** - Required: `Run(ctx) error`
2. **Readier** - Optional: `Ready() <-chan struct{}` for startup signaling
3. **Closeable** - Optional: `Close(ctx) error` for graceful shutdown
4. **ErrorNotifier** - Optional: `Errored() <-chan struct{}` for early error signaling

Use `NewRunner(runFunc, opts...)` for simple cases or `Looper(func(ctx, loop))` for indefinite loops.

## Development Workflows

### Build & Test
```bash
make test          # Run linters and unit tests
```

### Testing Patterns
- Use `gotest.tools/v3/assert` for assertions
- Test dependency resolution with `plumber.Resolved(&a.Dep1, &a.Dep2)` - returns `nil` if all resolve without errors
- Use `plumber.ContainerResolved(func() *Container { return NewApplication(...) })` to validate entire dependency graphs
- Test containers with custom definers: `NewApplication(ctx, cfg, WithTestOverrides)`

### Local Package Replacement
Test changes in downstream projects:
```bash
# In consuming project's go.mod:
replace github.com/getoutreach/plumber => /path/to/local/plumber
```

## Common Patterns in Examples

### Looper Pattern ([looper.go](looper.go))
For periodic tasks:
```go
plumber.Looper(func(ctx context.Context, l *plumber.Loop) error {
    l.Ready()  // Signal readiness immediately
    tick := time.Tick(500 * time.Millisecond)
    for {
        select {
        case <-tick:
            // Do work
        case done := <-l.Closing():
            // Cleanup
            done.Success()
            return nil
        }
    }
})
```

### Graceful Runner Pattern
For servers with startup/shutdown phases:
```go
plumber.GracefulRunner(
    func(ctx context.Context) error { /* Run */ },
    func(ctx context.Context) error { /* Close */ },
)
```

## Key Files to Reference
- [plumber.go](plumber.go) - Core `D[T]` dependency wrapper and resolution logic
- [orchestration.go](orchestration.go) - `Start()` and orchestration primitives
- [serial.go](serial.go) / [parallel.go](parallel.go) - Pipeline implementations
- [example/application.go](example/application.go) - Full container pattern example
- [example/cmd/example/main.go](example/cmd/example/main.go) - Complete orchestration example
- [docs/discover.md](docs/discover.md) - Dependency discovery patterns for code generation

## Project-Specific Conventions
- All new files must have copyright header: `// Copyright 2024 Outreach Corporation. All Rights Reserved.`
- Use `context.Context` consistently - never create contexts without passing parent
- Dependency fields in containers are public; resolution happens in `Define()` methods
- Test files use `_test` package suffix for black-box testing
- Use `Signal` type ([signal.go](signal.go)) for readiness/notification channels instead of raw channels
