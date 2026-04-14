# Plumber Shape

The `shape` command performs **annotation-driven code generation** over Go source files. It
scans packages for special `plumber:*` comment annotations, then either generates new Go
files or merges derived fields into existing structs — without requiring external templates
by default.

## Quick start

```shell
go run github.com/getoutreach/plumber/cmd/plumber@version shape [--config plumber.shape.yaml] ./...
```

`./...` follows standard Go package pattern syntax.  Pass a single package path (e.g.
`./internal/mypackage`) to restrict the scan.

---

## Annotation reference

Annotations are written in Go doc-comments, one per line, with the format:

```
// plumber:<option> [arg1 arg2 ...]
```

### Entry-point annotations

These open a new transformation block. Every subsequent annotation until the next entry
point (or end of type) is attached to this transformation.

| Annotation | Description |
|---|---|
| `plumber:derive` | Derive a new struct containing a filtered subset of the annotated struct's fields. Only works on `struct` types. |
| `plumber:shape` | Shape a type using a Go template. Works on both `struct` and `interface` types. |

### Modifier annotations

These refine the behaviour of the active transformation.

| Annotation | Args | Description |
|---|---|---|
| `plumber:name` | `<NewName>` | Name of the generated type or function. Defaults to the source type name. |
| `plumber:output` | `<file>` | Output filename relative to the source file's directory. Supports placeholders: `{filename}`, `{name}`, `{ext}`, `{suffix:<str>}`. Defaults to `generated.go`. |
| `plumber:mode` | `generated` \| `inplace` | Generation mode (see below). Defaults to `generated`. |
| `plumber:template` | `<template-name>` | Go template to apply. Can be specified multiple times. |
| `plumber:mixin` | `<mixin-name>` | Expands to a set of annotations defined in config under `plumber.shape.mixins`. |
| `plumber:filter` | `<fn> [arg...]` | Field filter predicate. E.g. `annotation.has is:filtrable` keeps only fields annotated with `is:filtrable`. |
| `plumber:ignore` | `<FieldName>` | Exclude a specific field from the output. |
| `plumber:field_wrapper` | `<wrapper-name>` | Apply a type wrapper (defined in config) to each included field's type. |
| `plumber:receiver` | `<ReceiverType>` | Override the receiver type for generated methods. |
| `plumber:comment` | `<text>` | Append a comment to the generated declaration. |
| `plumber:context` | `<pkg/Type>` | Used in **package-level comments** to point the transformation at a specific model type (fully qualified). |

---

## Modes

### `generated` (default)

Creates a brand-new Go file. The file header `// Generated file by plumber shape function. DON'T edit manually.`
is added automatically.  The output preserves `plumber::Block(...)` comment fences so that
hand-written extensions survive re-generation.

```go
// plumber:derive DerivedModel
// plumber:mixin  mixing.model.filtrable
// plumber:output generated.go
type Model struct {
    // Name
    //
    // is:filtrable
    Name string

    Concurrency int  // not filtrable — excluded by the mixin

    // Closer
    //
    // is:filtrable
    Closer OpenCloser
}
```

Produces `generated.go`:

```go
// Generated file by plumber shape function. DON'T edit manually.
package generated

// <<plumber::Block(header)>>
// <</plumber::Block>>

// DerivedModel is derived from "<pkg>".Model.
type DerivedModel struct {
    Name   string
    Closer OpenCloser
    // <<plumber::Block(extra-DerivedModel)>>
    // <</plumber::Block>>
}

// <<plumber::Block(footer)>>
// <</plumber::Block>>
```

### `inplace`

Merges the derived fields into an **existing struct** in the same package.  The target
struct must already exist; the command adds only fields that are not already present
(idempotent).

```go
// plumber:derive
// plumber:mode  inplace
// plumber:name  ModelBlended
type Model struct {
    Name        string
    Concurrency int
    Closer      OpenCloser
    Queues      []string
    Complex     complex.Complex
}
```

Before running `shape`, `blended.go` contains:

```go
type ModelBlended struct{}
```

After running `shape`, `blended.go` becomes:

```go
type ModelBlended struct {
    Name        string
    Concurrency int
    Closer      OpenCloser
    Queues      []string
    Complex     complex.Complex
}
```

The import block is updated automatically.

---

## Configuration file (`plumber.shape.yaml`)

The config file is passed via `--config` / `-c`.  At the top level it contains:

```yaml
# Pull in additional YAML files (glob patterns supported).
includes:
  - path: plumber.d/*.yaml

# Shape command config.
plumber.shape:
  workingDir: ""      # optional working directory override
  cacheDir:   ""      # optional cache directory for checked-out git templates

  # ---------- templates ----------
  templates:
    sources:
      # local template directory
      - local:
          path: ./templates
          templates:
            - name: plumber.template

      # git-hosted template
      - git:
          repository: https://github.com/example/templates
          ref: main
          templates:
            - name: remote.template
              path: scripts/remote.gtpl

    # inline template content (useful for simple cases)
    content:
      - name: plumber.template
        content: |
          // my inline template

  # ---------- mixins ----------
  # Mixins are named bundles of annotations that can be referenced with
  # plumber:mixin <name> in source code.
  mixins:
    - plumber.mixin:
        name: mixing.model.filtrable
        annotations:
          - name: plumber:filter
            args: [annotation.has, "is:filtrable"]
          - name: plumber:field_wrapper
            args: [model.filter]

    - plumber.mixin:
        name: mixing.model.accessor
        annotations:
          - name: plumber:template
            args: [plumber:object/accessor]
          - name: plumber:output
            args: [generated.go]

  # ---------- type wrappers ----------
  # Wrappers rewrite a field's type during generation, e.g. to wrap with a
  # generic container type based on the field's kind or FQN.
  type:
    wrappers:
      - plumber.wrapper:
          name: model.filter
          expressions:
            - plumber.wrapper_expression:
                # replacement type (fully-qualified)
                type: '"github.com/example/contract".Filtrable'
                matches:
                  - rule: 'fqn:"time".Time'
                  - rule: 'kind:interface'

# Inspect command config (used when running plumber inspect with the same file).
plumber.inspect:
  format: json
```

### Config hierarchy and `includes`

When `shape` loads a config file it:

1. Parses the root YAML.
2. Expands every glob listed under `includes[*].path` using `filepath.Glob`.
3. Parses each matched file independently.
4. Merges included configs into the root by **appending**:
   - `plumber.shape.templates.sources`
   - `plumber.shape.templates.content`
   - `plumber.shape.mixins`
   - `plumber.shape.type.wrappers`

This allows large projects to split mixin and wrapper definitions into per-module files
under a `plumber.d/` directory.

```
project/
├── plumber.shape.yaml          ← root, includes plumber.d/*.yaml
└── plumber.d/
    ├── mixins.yaml             ← defines shared mixins
    └── wrappers.yaml           ← defines shared wrappers
```

---

## Output filename placeholders

The `plumber:output` annotation value supports several placeholders that are expanded
relative to the source file:

| Placeholder | Expands to |
|---|---|
| `{filename}` | Full base filename of the source file, e.g. `model.go` |
| `{name}` | Filename without extension, e.g. `model` |
| `{ext}` | File extension including dot, e.g. `.go` |
| `{suffix:str}` | `{name}_str{ext}`, e.g. `{suffix:filter}` → `model_filter.go` |

---

## Acceptance tests

The acceptance test suite lives under `test/acceptance/` and exercises the two main modes:

| Test | Fixture | Mode | Verifies |
|---|---|---|---|
| `TestGenerated` | `fixture/generated/` | `generated` | Mixin + filter annotations produce the correct `generated/generated.go` |
| `TestMerge` | `fixture/merge/` | `inplace` | Inplace derive merges all fields from `Model` into the empty `ModelBlended` struct |

Golden files are stored under `fixture/assert/` and compared byte-for-byte (after
normalising the temporary directory name in import paths).
