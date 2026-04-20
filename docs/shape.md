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

### Single-type mode

Instead of scanning all types for annotations, you can target a specific type and apply a
named macro programmatically:

```shell
go run github.com/getoutreach/plumber/cmd/plumber@version shape \
  --config plumber.shape.yaml \
  --type '"github.com/example/pkg".MyService' \
  --macro '@derive' \
  --macro-arg DerivedName \
  --macro-arg AnotherArg \
  --macro-named-arg file=generated.go \
  --macro-named-arg mode=inplace \
  ./...
```

| Flag | Type | Description |
|---|---|---|
| `--type` | string | FQN of the target type (or unqualified name if unique) |
| `--macro` | string | Macro name to apply (must exist in config) |
| `--macro-arg` | string (repeatable) | Positional arg passed to the macro |
| `--macro-named-arg` | string (repeatable) | Named arg as `key=value` passed to the macro |

When `--type` and `--macro` are both provided, the command operates in **exclusive mode**:
it bypasses the annotation scan and only processes the specified type with the named macro.
The package pattern (`./...`) is still required so that the type can be resolved.

The type can be specified as:
- A **fully-qualified name**: `"github.com/example/pkg".MyService`
- An **unqualified name**: `MyService` (matches the first type with that name across all
  loaded packages)

The macro is expanded with the provided `--macro-arg` and `--macro-named-arg` values as
its `.Args` and `.NamedArgs` template context, exactly as if the type had a
`// @<macro-name> arg1 arg2 key=value` annotation in source code.

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
| `plumber:scope` | `"<Name>" type="<FQN>"` | Inject a resolved type into the template scope under `.Scope.Custom.<Name>`. Can be specified multiple times. |

---

### Custom scope (`plumber:scope`)

`plumber:scope` injects additional resolved types into the template rendering scope,
making them available as `.Scope.Custom.<Name>`. This is useful when a template needs
to reference types beyond the subject type being transformed.

```go
// plumber:shape
// plumber:template my-adapter
// plumber:scope "Target" type="github.com/example/pkg".TargetService
// plumber:scope "Config" type="github.com/example/pkg".AdapterConfig
type MyAdapter struct { ... }
```

Inside the template, the resolved types are accessible as full `*model.Type` values:

```
{{ .Scope.Custom.Target.Struct.Fields }}
{{ .Scope.Custom.Config.Spec.Name }}
```

Each `plumber:scope` annotation requires:
- A **positional argument** — the key name under `.Scope.Custom`
- A **`type=` named argument** — a fully-qualified Go type name (FQN)

The FQN must reference a type that is present in the inspected packages. Multiple
`plumber:scope` annotations can appear on the same transformation.

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

#### Inplace merge mechanics

When `plumber:mode inplace` is set, the shape command renders the template into a
temporary Go file, parses it with a DST decorator, and merges each declaration into the
existing file.  The merge is **idempotent** — running the command twice produces the same
result.  Every merge operation adds what is missing without removing anything the user
has written.

##### Struct fields

Template struct fields are compared against the existing struct by **field name**.  Missing
fields are appended; existing fields are left unchanged (type, tags, and comments are
preserved as-is).

```
Template:                      Existing:                 Result:
type S struct {                type S struct {           type S struct {
    A int                          A string                  A string      ← kept
    B string                       C bool                    C bool        ← kept
}                              }                             B string      ← added
                                                         }
```

##### Functions and methods

Functions and methods are matched by **name only** (receiver type is used to locate
methods on the correct struct, but the receiver variable name is ignored).

| Existing state | Behaviour |
|---|---|
| Function does not exist | Entire declaration is added to the file |
| Function exists with empty body | All template body statements are inserted |
| Function exists with non-empty body | Template statements must appear as an **ordered subsequence** (see body merge below) |

**Parameters** are merged positionally: template parameters must be a prefix of the
existing parameter list.  Extra parameters in the existing function are fine.  Missing
template parameters are appended.

##### Variables

Top-level `var` declarations are matched by **variable name**.  If the variable does not
exist it is added; if it already exists it is skipped entirely.

##### Body merge (statement-by-statement)

When a function already has a non-empty body, the template's statements must appear as an
**ordered subsequence** of the existing body.  This means every template statement must be
found in the existing body in the same relative order, but the existing body may contain
additional statements between them.

If a template statement cannot be matched, the merge fails with an error — a "removed
statement" is treated as an intentional user change that the template must not silently
re-introduce.

Statements are matched by a **shallow key** (structural identity), then **deep-merged**
to augment nested expressions:

| Statement type | Match key |
|---|---|
| Assignment (`x := ...`) | LHS expression(s) must match by expression key |
| Expression statement (call) | Call target function name must match |
| Return | Always matches any other return (keyword match) |
| Declaration (`var x ...`) | Variable name(s) must match |
| Switch | Tag expression must match |
| If / For / Range | Same Go statement type (type match) |

##### Deep merge of matched statements

After a statement is matched, its contents are **recursively deep-merged** to ensure
template-required arguments and fields are present:

**Call arguments** — template arguments must all be present in the existing call.
Arguments are compared structurally by expression key.  Extra existing arguments are
preserved; missing template arguments are appended.

```go
// Template:                        Existing:
s.Logger.Info("starting", "svc")    s.Logger.Info("starting")
// Result:
s.Logger.Info("starting", "svc")    // "svc" appended
```

**Composite literals** — template key-value entries must all be present.  Entries are
matched by key name.  Extra existing entries are preserved; missing entries are appended.
Values of matched entries are deep-merged recursively.

```go
// Template:                        Existing:
&Config{                            &Config{
    A: 1,                               A: 1,
    B: 2,                               C: 3,
}                                   }
// Result:
&Config{A: 1, C: 3, B: 2}          // B appended, C preserved
```

Deep merge is **recursive** — it applies at any depth in the AST.  A composite literal
nested inside a call argument inside a return statement will still be augmented.

##### Switch statement case merge

Switch statements are matched by their **tag expression** (the value being switched on).
Once matched, case clauses are merged:

- Each template `case` and `default` clause must be present in the existing switch.
- Cases are matched by their **case expression values** (e.g., `"case1"`, `"case2"`).
  The `default` clause is matched by having an empty case expression list.
- **Missing cases** from the template are inserted after the last matched preceding case,
  preserving the relative order from the template.
- **Extra existing cases** are preserved in place.
- The **body of matched cases** is deep-merged using the same call-argument and
  composite-literal augmentation as function bodies.

```
Template:                  Existing:                Result:
switch s {                 switch s {               switch s {
case "a":                  case "a":                case "a":       ← matched
    foo()                      foo()                    foo()
case "b":                  case "c":                case "b":       ← inserted
    bar()                      baz()                    bar()
default:                   default:                 case "c":       ← preserved
    fallback()                 fallback()                baz()
}                          }                        default:        ← matched
                                                        fallback()
                                                    }
```

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

  # ---------- sources ----------
  # Template sources (local or git). These are resolved during template loading.
  sources:
    # local template directory
    - local:
        path: ./templates
        templates:
          - name: plumber.template

    # git-hosted template (sparse-cloned into cacheDir)
    - git:
        repository: https://github.com/example/templates
        ref: main
        # includes: load additional config files from within the git repo
        includes:
          - path: plumber.d/*.yaml
        templates:
          - name: remote.template
            path: scripts/remote.gtpl

  # ---------- templates ----------
  # Inline template content (useful for simple cases).
  templates:
    content:
      - name: plumber.template
        content: |
          // my inline template

  # ---------- macros ----------
  # Macros are named bundles of annotations expanded early (before transformer
  # building), allowing injection of any annotation including entry-point
  # annotations like plumber:derive and plumber:shape.
  # Referenced in Go source with the @<name> syntax.
  macros:
    - plumber.macro:
        name: "@derive"
        annotations:
          - name: plumber:derive
            args: ["MacroDerived"]
          - name: plumber:output
            args: ["{suffix:generated}"]

  # ---------- mixins ----------
  # Mixins are named bundles of modifier annotations that can be referenced
  # with plumber:mixin <name> in source code. They are expanded inside
  # transformer building and can only inject modifier annotations.
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
   - `plumber.shape.sources`
   - `plumber.shape.templates.content`
   - `plumber.shape.macros`
   - `plumber.shape.mixins`
   - `plumber.shape.type.wrappers`

This allows large projects to split macro, mixin, and wrapper definitions into per-module
files under a `plumber.d/` directory.

```
project/
├── plumber.shape.yaml          ← root, includes plumber.d/*.yaml
└── plumber.d/
    ├── macros.yaml             ← defines shared macros
    ├── mixins.yaml             ← defines shared mixins
    └── wrappers.yaml           ← defines shared wrappers
```

### Git source `includes`

Git sources can declare their own `includes` — glob patterns pointing to YAML config files
within the checked-out repository.  After the repo is sparse-cloned, matching files are
parsed as full shape configs and merged into the running config with the same semantics as
root-level `includes`.

```yaml
sources:
  - git:
      repository: https://github.com/example/templates
      ref: main
      includes:
        - path: plumber.d/*.yaml    # paths relative to the repo root
      templates:
        - name: remote.template
          path: scripts/remote.gtpl
```

This allows shared template repositories to ship their own mixin, macro, and wrapper
definitions alongside the templates.

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

## Mixins

Mixins are reusable bundles of **modifier annotations** defined in config and referenced
in Go source with `plumber:mixin <name>`.  They are expanded **inside** the
transformer-building stage — after an entry-point annotation (`plumber:shape` or
`plumber:derive`) has created a transformer.  This means mixins can only inject modifier
annotations that a transformer accepts (e.g. `plumber:filter`, `plumber:template`,
`plumber:output`, `plumber:field_wrapper`).

### Defining a mixin

```yaml
mixins:
  - plumber.mixin:
      name: mixing.model.filtrable
      annotations:
        - { name: plumber:filter, args: [annotation.has, "is:filtrable"] }
        - { name: plumber:field_wrapper, args: [model.filter] }
```

### Using a mixin

```go
// plumber:derive
// plumber:name WorkerFilter
// plumber:mixin mixing.model.filtrable
// plumber:output {suffix:generated}
type Worker struct {
    // Name
    //
    // is:filtrable
    Name string

    Concurrency int  // not filtrable — excluded by the mixin filter
}
```

When `buildTransformers()` encounters `plumber:mixin mixing.model.filtrable`, it:

1. Looks up the mixin by name in `config.Mixins`.
2. Validates each of the mixin's annotations against the current transformer via `Accepts()`.
3. Adds each annotation to the active transformer.

A single type can reference multiple mixins across different transformer blocks.

---

## Macros

Macros are named bundles of annotations that expand **before** transformer building,
allowing injection of **any** annotation — including entry-point annotations like
`plumber:derive` and `plumber:shape` that create new transformers.  This is the key
difference from mixins, which can only inject modifier annotations.

Macros are referenced in Go source using the `@<name>` syntax.

### Defining a macro

```yaml
macros:
  - plumber.macro:
      name: "@derive"
      annotations:
        - { name: plumber:derive, args: ["MacroDerived"] }
        - { name: plumber:output, args: ["{suffix:generated}"] }
```

### Using a macro

```go
// @derive
type Worker struct {
    Name        string
    Concurrency int
}
```

At runtime, `expandMacros()` replaces the `@derive` annotation with `plumber:derive
MacroDerived` + `plumber:output {suffix:generated}` on the node before any transformer
building occurs.  The result is a `generated` mode derive that produces
`worker_generated.go` containing a `MacroDerived` struct.

### Template expansion

Macro annotation values support Go `text/template` syntax, giving macros access to the
arguments passed at the call site. The template data context exposes:

| Field       | Type                | Description                                           |
|-------------|---------------------|-------------------------------------------------------|
| `.Args`     | `[]string`          | Positional arguments from the triggering annotation   |
| `.NamedArgs`| `map[string]string` | Named arguments (`key=value`) from the triggering annotation |

#### Defining a macro with templates

```yaml
macros:
  - plumber.macro:
      name: "@tderive"
      annotations:
        - { name: plumber:derive, args: ["{{ index .Args 0 }}"] }
        - { name: plumber:output, args: ["{{ .NamedArgs.file }}"] }
```

#### Using a macro with arguments

```go
// @tderive Widget file=generated.go
type Order struct {
    ID    string
    Total int
}
```

This expands to `plumber:derive Widget` + `plumber:output generated.go`, producing a
`Widget` struct derived from `Order`.

Strings that do not contain `{{` are passed through unchanged, so the existing
`{name}` / `{suffix:...}` placeholder syntax used by transformers is unaffected.
Template errors (e.g. referencing a missing key) cause a hard failure and abort the
pipeline.

### Macros vs mixins

| | Macros | Mixins |
|---|---|---|
| Source syntax | `@<name>` | `plumber:mixin <name>` |
| Expansion stage | Before `Walk` (early) | Inside `buildTransformers` (late) |
| Can inject entry-point annotations | Yes | No |
| Config key | `macros` | `mixins` |

---

## Queries

The `plumber:query` annotation populates a slice variable with entities (functions, types,
variables) matching a regex pattern within a given scope. Queries run after template
rendering and modify the source file in-place via DST manipulation.

### Annotation syntax

```
plumber:query "<regex>" scope="<scope>" [receiver="<var>"]
```

- **`<regex>`** — Go regular expression matched against entity names.
- **`scope`** (required) — Where to search. Supported forms:
  - `"."` — current package
  - `".TypeName"` — fields/methods of a type in the current package
  - `"./relpath"` — relative package path
  - `"github.com/pkg"` — external package
  - `"github.com/pkg.TypeName"` — type in an external package
- **`receiver`** (optional, required for type-scoped) — variable name to qualify field/method access.

### Package-level variables

```go
// plumber:query "^Init.*" scope="."
var InitFunctions = []func(){}
```

After processing, `InitFunctions` is populated with all exported `func()` entities whose
name matches `^Init.*` in the current package.

### Function-body variables

Queries also work on variables declared inside function and method bodies:

```go
func Setup() {
    // plumber:query "^Init.*" scope="."
    var initFuncs = []func(){}
    for _, f := range initFuncs {
        f()
    }
}
```

The comment annotation is placed directly above the `var` declaration inside the function
body. Only explicit `var` declarations with a composite literal value are supported
(short `:=` declarations are not).

### Type-scoped queries

```go
var r Registry

// plumber:query "^Get.*" scope=".Registry" receiver="r"
var Getters = []func() string{}
```

This populates `Getters` with `r.GetAlpha`, `r.GetBeta`, etc. — all fields/methods of
`Registry` matching `^Get.*` with a compatible `func() string` signature.

### Cross-package queries

```go
// plumber:query "^Init.*" scope="./providers"
var InitFunctions = []func(){}
```

Results from external packages use qualified identifiers (`providers.InitAlpha`) and the
import is automatically managed by the DST restorer.
