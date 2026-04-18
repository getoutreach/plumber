# Dependency Discovery

Plumber's dependency management is type-safe and declarative, but wiring containers by
hand is verbose — every provider needs a struct field, a `Resolver` block, `Require()`
declarations, and correct imports. Discovery automates this by scanning Go source code
for constructor functions and generating the container wiring automatically.

Discovery operates at code-generation time using a Go AST parser. Given a YAML
configuration file, it:

1. Scans source directories for constructor functions matching regex patterns.
2. Adds missing `plumber.D[T]` fields to container structs.
3. Generates `Resolver` blocks that call the discovered constructors.
4. Auto-wires constructor arguments from the dependency graph.
5. Auto-populates `Require()` with dependencies used inside `Then()` callbacks.
6. Manages imports for all generated code.

---

## Usage

Run discovery from the project root:

```bash
go run cmd/plumber/plumber.go discovery --config plumber.yaml
```

| Flag | Alias | Required | Description |
|---|---|---|---|
| `--config` | `-c` | Yes | Path to the `plumber.yaml` configuration file |

Discovery reads the config, expands any `loop` directives, scans source directories,
and augments container files in place. It prints a summary of discovered providers and
any changes made.

---

## Configuration (`plumber.yaml`)

Discovery is driven by a `plumber.yaml` file at the application root. It declares which
source directories to scan, what constructor patterns to match, and where to write the
generated container files.

```yaml
applications:
  - name: application
    # Fully qualified Go module path
    module: github.com/getoutreach/plumber/example
    # Config struct type (quoted import path for external packages)
    config: '*"github.com/getoutreach/plumber/example".Config'

    containers:
      # Container with loop — generates one container per matched directory
      - plumber.container:
          comment: "Adapter modules"
          name: "{{ .module | capitalize }}"
          container:
            path: ./application_{{ .module }}.go
          source:
            path: ./adapter/{{ .module }}/
          matchers:
            - constructors:
                - New(?P<name>.*)
                - Factory(?P<name>.*)
        loop:
          path: adapter/(?P<module>\w+)

      # Static container — no loop, scans a single directory
      - plumber.container:
          name: "Scenario"
          container:
            path: ./application_scenario.go
          source:
            path: ./scenario/
          matchers:
            - constructors:
                - New(?P<name>.*)

# Template for generating new container files when they don't exist yet
templates:
  container: |
    package {{ .package_name }}

    import (
      "context"
    )

    // {{ .container.name }} dependency container
    type {{ .container.name }} struct {}

    // Define dependency resolvers
    func (c *{{ .container.name }}) Define(ctx context.Context, cf {{ .config.type }}, a *Container) {
    }
```

### Schema reference

| Field | Description |
|---|---|
| `applications[].name` | Application identifier |
| `applications[].module` | Go module path for the application |
| `applications[].config` | Fully qualified config struct type |
| `containers[].plumber.container.name` | Container struct name (supports template variables) |
| `containers[].plumber.container.comment` | Comment added to the generated container |
| `containers[].plumber.container.container.path` | Output file path for the container (supports template variables) |
| `containers[].plumber.container.source.path` | Source directory to scan for constructors (supports template variables) |
| `containers[].plumber.container.matchers[].constructors` | Regex patterns for matching constructor functions |
| `containers[].loop.path` | Regex with named capture groups for directory iteration |
| `templates.container` | Go template for generating new container file skeletons |

---

## Provider discovery

Discovery scans source directories for Go functions that match the configured constructor
patterns. It uses the Go AST and type system (`go/packages`) to extract full type
information.

A function is considered a constructor when:

1. It is a **free function** (not a method with a receiver).
2. It returns **one or two values** — the provider type, and optionally an `error`.
3. Its name matches one of the configured regex patterns.

The `(?P<name>...)` named capture group in the regex extracts the **provider name** used
for the struct field.

### Example

Given this constructor in `adapter/async/publisher.go`:

```go
func NewPublisher(broker string) *Publisher {
    return &Publisher{broker: broker}
}
```

And this matcher pattern:

```yaml
constructors:
  - New(?P<name>.*)
```

Discovery produces a provider:
- **Name**: `Publisher` (from the capture group)
- **Type**: `*async.Publisher` (from the return type)
- **Constructor**: `NewPublisher` with parameter `broker string`

---

## Code generation (augmentation)

For each discovered provider that is not already present in the container struct,
discovery generates two things: a struct field and a resolver block.

### Struct fields

Missing providers are added as `plumber.D[T]` fields:

```go
type Async struct {
    Publisher plumber.D[*async.Publisher]  // generated
}
```

### Resolver blocks

A `Resolver` block is appended to the container's `Define` method:

```go
func (c *Async) Define(ctx context.Context, cf *Config, a *Container) {
    c.Publisher.Resolver(func(r *plumber.Resolution[*async.Publisher]) {
        r.Require().Then(func() {
            r.Resolve(async.NewPublisher(
                // constructor arguments wired here
            ))
        })
    })
}
```

If the constructor returns `(T, error)`, `ResolveError` is used instead of `Resolve`.

### New container files

When a container file does not exist yet, discovery creates it from the `templates.container`
template before augmentation. The template receives context variables including
`package_name`, `container.name`, and `config.type`.

---

## Automatic dependency wiring

Discovery builds a **global provider map** that indexes every discovered provider by its
Go type. When generating constructor calls, each parameter type is looked up in this map
to determine how to wire it.

### Exact match — single provider

When exactly one provider produces the required type, discovery wires it directly:

```go
// Parameter type *async.Publisher is provided by a.Async.Publisher
r.Resolve(scenario.NewScenario(
    a.Async.Publisher.Instance(),
))
```

Cross-container references use the root container (`a.OtherContainer.Field.Instance()`),
while same-container references use the receiver (`c.Field.Instance()`).

### Ambiguous match — multiple providers

When multiple providers produce the same type, discovery cannot choose automatically.
It generates a `discovery.OneOf(...)` call listing all candidates:

```go
r.Resolve(example.NewService(
    discovery.OneOf(
        a.Async.Publisher.Instance(),
        a.Async.DelayedPublisher.Instance(),
    ),
))
```

`OneOf` panics at runtime with a descriptive message — it is a placeholder that must be
replaced by selecting the correct provider manually.

### No match — undefined dependency

When no provider produces the required type (e.g., a scalar like `string` or `int`),
discovery generates `discovery.Undefined[T]()`:

```go
r.Resolve(async.NewPublisher(
    discovery.Undefined[string](),
))
```

`Undefined` panics at runtime — it signals that the dependency must be provided manually
(typically via `Const` or a config value).

---

## Auto-fixing `Require()`

Discovery automatically populates `Require()` arguments by scanning the `Then()` callback
body. Any call to `.Instance()` or `.InstanceError()` on a dependency field is detected,
and the corresponding `&c.Field` or `&a.Container.Field` reference is added to `Require()`.

### Before (generated with empty `Require`)

```go
c.Scenario.Resolver(func(r *plumber.Resolution[*scenario.Scenario]) {
    r.Require().Then(func() {
        r.Resolve(scenario.NewScenario(
            a.Async.Publisher.Instance(),
        ))
    })
})
```

### After (auto-fixed)

```go
c.Scenario.Resolver(func(r *plumber.Resolution[*scenario.Scenario]) {
    r.Require(
        &a.Async.Publisher,
    ).Then(func() {
        r.Resolve(scenario.NewScenario(
            a.Async.Publisher.Instance(),
        ))
    })
})
```

This works for both newly generated resolvers and existing hand-written ones. Already
declared requirements are preserved — only missing ones are added.

---

## Sentinel functions

The `discovery` package provides sentinel functions used in generated code to mark
unresolved wiring. These compile successfully but panic at runtime with descriptive
messages, making problems visible during testing via `ContainerResolved`.

| Function | Generated when | Message |
|---|---|---|
| `discovery.Undefined[T]()` | No provider found for a scalar/non-discovered type | `undefined dependency of type T` |
| `discovery.OneOf[T](...)` | Multiple providers match the required type | `unselected one of dependency of type T` |
| `discovery.Unresolved[T]()` | Dependency has not been resolved | `unresolved dependency of type T` |
| `discovery.UndeclaredDependency[T](v)` | Dependency used but not declared | `undeclared dependency` |

These sentinels are designed to fail fast during container validation (see
[ContainerResolved](dependency-management.md#containerresolved--graph-validation) in the
dependency management docs).

---

## Loop expansion

The `loop` directive generates multiple containers from a directory structure. It uses
a regex with named capture groups to extract variables from directory paths, then
hydrates the container config template for each match.

### Example

Given this directory layout:

```
adapter/
  async/
  database/
  graphql/
  grpc/
```

And this config:

```yaml
- plumber.container:
    name: "{{ .module | capitalize }}"
    container:
      path: ./application_{{ .module }}.go
    source:
      path: ./adapter/{{ .module }}/
    matchers:
      - constructors:
          - New(?P<name>.*)
  loop:
    path: adapter/(?P<module>\w+)
```

Discovery expands this into four containers:

| Variable `module` | Container name | Output file | Source path |
|---|---|---|---|
| `async` | `Async` | `./application_async.go` | `./adapter/async/` |
| `database` | `Database` | `./application_database.go` | `./adapter/database/` |
| `graphql` | `Graphql` | `./application_graphql.go` | `./adapter/graphql/` |
| `grpc` | `Grpc` | `./application_grpc.go` | `./adapter/grpc/` |

Each expanded container is processed independently — providers are discovered from its
source path and the container file is augmented accordingly.

---

## Template variables

Template strings in the config (`name`, `container.path`, `source.path`) and the
`templates.container` block support Go `text/template` syntax with the following
variables and helpers.

### Variables in container config (from loop)

Variables are extracted from the `loop.path` regex named capture groups. In the example
above, `(?P<module>\w+)` produces a `module` variable.

### Variables in `templates.container`

| Variable | Description |
|---|---|
| `{{ .package_name }}` | Go package name for the container file |
| `{{ .container.name }}` | Container struct name |
| `{{ .container.module }}` | Container module identifier |
| `{{ .config.type }}` | Config struct type expression |
| `{{ .config.module }}` | Config package import path |
| `{{ .config.remote }}` | Whether the config is from an external module |

### Helper functions

| Helper | Description | Example |
|---|---|---|
| `capitalize` | Uppercase first letter | `{{ .module \| capitalize }}` → `Async` |
| `upper` | All uppercase | `{{ .module \| upper }}` → `ASYNC` |
| `lower` | All lowercase | `{{ .module \| lower }}` → `async` |
| `title` | Title case | `{{ .module \| title }}` → `Async` |

---

## End-to-end flow

```
plumber.yaml
    │
    ▼
Parse config
    │
    ▼
Expand loops (scan directories, hydrate templates)
    │
    ▼
For each container:
    │
    ├─ Container file missing?
    │     → Generate skeleton from templates.container
    │
    ├─ Scan source path with AST parser
    │     → Match constructors against regex patterns
    │     → Extract provider name, type, and parameters
    │
    └─ Augment container file
          ├─ Add missing plumber.D[T] struct fields
          ├─ Generate Resolver blocks with constructor calls
          ├─ Wire arguments from provider map
          │     (single match → .Instance(), multiple → OneOf, none → Undefined)
          ├─ Auto-populate Require() from .Instance() usage
          ├─ Manage imports
          └─ Write modified file
```
