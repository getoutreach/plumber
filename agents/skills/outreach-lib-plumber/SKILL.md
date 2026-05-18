---
name: outreach-lib-plumber
description: Provides guidance on using the plumber Go library for dependency injection, task orchestration, code discovery, shape-based code generation, and codebase inspection.
---

# Plumber Library

Plumber is a Go library for building applications with type-safe dependency injection,
lifecycle-managed task orchestration, and code generation tooling. It ships as both a
library (`github.com/getoutreach/plumber`) and a CLI (`cmd/plumber/plumber.go`).

## Feature areas

Each feature is documented in a dedicated file. Read the relevant file for the task at
hand — each is self-contained.

| Feature | File | Use when... |
|---|---|---|
| **Dependency injection** | [dependency-injection.md](dependency-injection.md) | You need to understand mechanics of dependency declaration of service dependencies with the project. Details of using `D[T]`, `R[T]`, structure of the application graph,containers, and resolvers. |
| **Discovery** | [discovery.md](discovery.md) | Container wiring is repetitive and follows constructor conventions — let the CLI auto-generate container struct fields, resolvers, and dependency wiring from a YAML config. |
| **Task orchestration** | [task-orchestration.md](task-orchestration.md) | You need to start, sequence, and gracefully shut down long-running services using `Pipeline`, `Parallel`, runners, and lifecycle options. |
| **Shape** | [shape.md](shape.md) | You need annotation-driven code generation: deriving filtered structs, rendering Go templates, or populating slices from pattern-matched entities. |
| **Inspect** | [inspect.md](inspect.md) | You need structured JSON/YAML output describing packages, types, functions, and annotations for external tooling or analysis. |

## Decision guide

- **Building a new service with dependencies?** Start with [dependency-injection.md](dependency-injection.md) to set up containers, then [task-orchestration.md](task-orchestration.md) to wire the startup pipeline.
- **Container wiring is getting verbose?** Use [discovery.md](discovery.md) to auto-generate the boilerplate from constructor conventions.
- **Need to derive types, generate adapter code, or transform structs?** Use [shape.md](shape.md) with `plumber:derive` or `plumber:shape` annotations.
- **Need codebase metadata for tooling?** Use [inspect.md](inspect.md) to get structured type information.

All features are independent. You can use DI without discovery, shape without DI, or
inspect standalone. They compose but do not require each other.

## Import paths

| Package | Purpose |
|---|---|
| `github.com/getoutreach/plumber` | Core library: `D[T]`, `R[T]`, `Pipeline`, `Start`, etc. |
| `github.com/getoutreach/plumber/discovery` | Runtime sentinels for generated code: `Undefined`, `OneOf`, etc. |

## CLI

The plumber CLI lives at `github.com/getoutreach/plumber/cmd/plumber`:

```bash
go run github.com/getoutreach/plumber/cmd/plumber@latest <command> [flags]
```

| Command | Description |
|---|---|
| `discovery --config plumber.yaml` | Auto-discover providers and generate container wiring |
| `shape [--config plumber.shape.yaml] ./...` | Run annotation-driven code generation |
| `inspect [--format json\|yaml] ./...` | Emit structured codebase metadata |
