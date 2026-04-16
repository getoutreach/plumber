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

Declarative dependency resolution that eliminates repetitive error checking during construction. Dependencies are built once and reused across the application graph.

[Read more](docs/dependency-management.md)

### Service task orchestration

Lifecycle management for multi-layered applications. Start tasks in a defined order using serial pipelines or run independent tasks in parallel, with graceful shutdown in reverse order.

[Read more](docs/task-orchestration.md)

### Code manipulation

Annotation-driven code generation over Go source files. Scan packages for `plumber:*` comment annotations, then generate new Go files or merge derived fields into existing structs.

- [Shape command](docs/shape.md) -- annotation-driven code generation
- [Inspect command](docs/inspect.md) -- structured type information output
