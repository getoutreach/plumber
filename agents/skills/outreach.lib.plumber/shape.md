# Shape — Annotation-Driven Code Generation

The `shape` command scans Go packages for `plumber:*` comment annotations and generates or
transforms code — deriving filtered structs, rendering Go templates, or populating slices
from pattern-matched entities.

## When to use

- Deriving a subset struct from an existing struct (e.g., filter models, API DTOs).
- Generating boilerplate code from templates based on type metadata.
- Populating slices with functions/fields matching a regex pattern (`plumber:query`).
- Merging derived fields into existing structs (inplace mode).

## CLI

### Running using mise (preferred when project is managed by mise)

```bash
mise exec -- plumber shape [--config plumber.yaml] ./...
```

### Running using remove path

```bash
go run github.com/getoutreach/outreach/plumber@latest/cmd/plumber shape [--config plumber.yaml] ./...
```

| Flag | Alias | Default | Description |
|---|---|---|---|
| `--config` | `-c` | — | Path to `plumber.shape.yaml` (optional) |
| `--type` | — | — | FQN or unqualified name of target type (single-type mode) |
| `--macro` | — | — | Macro name to apply (requires `--type`) |
| `--macro-arg` | — | — | Positional arg for the macro (repeatable) |
| `--macro-named-arg` | — | — | Named arg as `key=value` (repeatable) |

Standard Go package patterns are supported: `./...`, `./internal/pkg`, etc.

### Single-type mode

When `--type` and `--macro` are both set, the command skips annotation scanning and
processes only the specified type with the named macro. The macro must exist in config.

```bash
go run cmd/plumber/plumber.go shape \
  --config plumber.shape.yaml \
  --type Worker \
  --macro '@derive' \
  --macro-arg DerivedName \
  --macro-named-arg mode=inplace \
  ./...
```

The type can be a full FQN (`"github.com/pkg".Type`) or an unqualified name (`Type`).

## Annotations

Annotations are written in Go doc-comments:

```go
// plumber:<option> [arg1 arg2 ...]
```

### Entry-point annotations

These start a new transformation block:

| Annotation | Description |
|---|---|
| `plumber:derive` | Derive a new struct with a filtered subset of fields. Struct types only. |
| `plumber:shape` | Shape a type using a Go template. Works on structs and interfaces. |

### Modifier annotations

These refine the active transformation:

| Annotation | Args | Description |
|---|---|---|
| `plumber:name` | `<Name>` | Name of the generated type/function. |
| `plumber:output` | `<file>` | Output filename. Rendered as a Go `text/template` exposing `.Filename`, `.Name`, `.Ext`, and the `suffixed` helper. |
| `plumber:mode` | `generated` \| `inplace` | Generation mode. |
| `plumber:template` | `<name>` | Go template to apply. Can be repeated. |
| `plumber:mixin` | `<name>` | Expand a named mixin (modifier annotation bundle from config). |
| `plumber:filter` | `<fn> [arg...]` | Field filter predicate. E.g., `annotation.has is:filtrable`. |
| `plumber:ignore` | `<FieldName>` | Exclude a specific field. |
| `plumber:field_wrapper` | `<name>` | Apply a type wrapper to each included field. |
| `plumber:receiver` | `<Type>` | Override receiver type for generated methods. |
| `plumber:comment` | `<text>` | Append comment to the generated declaration. |
| `plumber:context` | `<pkg/Type>` | Package-level: point transformation at a specific model type. |
| `plumber:scope` | `"<Name>" type="<FQN>"` | Inject a resolved type into template scope as `.Scope.Custom.<Name>`. |
| `plumber:depends_on` | `<FQN>` | Silently skip the transformation when the FQN cannot be resolved in the inspected packages. May appear multiple times — all dependencies must resolve. |

## Modes

### `generated` (default)

Creates a new Go file with a `// Generated file...` header. Preserves `plumber::Block(...)`
comment fences for hand-written extensions:

```go
// plumber:derive DerivedModel
// plumber:filter annotation.has is:filtrable
// plumber:output generated.go
type Model struct {
    // Name
    //
    // is:filtrable
    Name string

    Concurrency int  // excluded — not filtrable
}
```

Produces:

```go
// Generated file by plumber shape function. DON'T edit manually.
type DerivedModel struct {
    Name string
    // <<plumber::Block(extra-DerivedModel)>>
    // <</plumber::Block>>
}
```

**Do not edit lines outside `plumber::Block` fences** — they will be overwritten on
re-generation.

### `inplace`

Merges derived fields into an existing struct. Only adds fields that are not already
present (idempotent). Imports are managed automatically.

If the target type does not yet exist in the package, the generated declaration is
appended to the file named by `plumber:output` (defaults to `generated.go`). The file
is created on demand, so inplace mode is safe to use for both initial generation and
subsequent merges.

```go
// plumber:derive
// plumber:mode  inplace
// plumber:name  ModelBlended
type Model struct {
    Name   string
    Closer OpenCloser
}
```

#### Inplace merge mechanics

The merge is idempotent — running twice produces the same result. It adds what is missing
without removing anything the user has written.

**Struct fields:** Matched by field name. Missing fields appended; existing fields
preserved as-is (type, tags, comments).

**Functions/methods:** Matched by name. Missing functions are added entirely. Existing
functions with empty bodies receive all template statements. Existing functions with
non-empty bodies require template statements as an **ordered subsequence** — if a template
statement is missing from the existing body, the merge fails (removed statements are
treated as intentional user changes). Parameters are merged positionally (template params
must be a prefix; missing ones are appended).

**Variables:** Matched by name. Added if missing, skipped if exists.

**Statement matching (shallow key):**

| Statement type | Match key |
|---|---|
| Assignment | LHS expression(s) |
| Expression (call) | Call target function name |
| Return | Keyword (always matches) |
| Declaration | Variable name(s) |
| Switch | Tag expression |
| If / For / Range | Same Go type |

**Deep merge of matched statements:**
- **Call arguments:** template args must be present; extra existing args preserved; missing appended.
- **Composite literals:** template key-value entries must be present; matched by key name; missing appended. Recursive at any AST depth.
- **Switch cases:** cases matched by expression values; missing template cases inserted after last matched preceding case; extra existing cases preserved; matched case bodies deep-merged.

## Output filename templates

`plumber:output` is rendered as a Go `text/template`. Plain values without `{{` are
returned verbatim.

| Expression                | Expands to |
|---------------------------|---|
| `{{ .Filename }}`         | Full base filename, e.g., `model.go` |
| `{{ .Name }}`             | Filename without extension, e.g., `model` |
| `{{ .Ext }}`              | Extension including dot, e.g., `.go` |
| `{{ suffixed "str" }}`    | `<.Name>_str<.Ext>`, e.g., `{{ suffixed "filter" }}` -> `model_filter.go` |

## Configuration (`plumber.shape.yaml`)

```yaml
includes:
  - path: plumber.d/*.yaml

plumber.shape:
  sources:
    - local:
        path: ./templates
        templates:
          - name: plumber.template
    - git:
        repository: https://github.com/example/templates
        ref: main
        includes:
          - path: plumber.d/*.yaml
        templates:
          - name: remote.template
            path: scripts/remote.gtpl

  templates:
    content:
      - name: plumber.template
        content: |
          // inline template content

  macros:
    - plumber.macro:
        name: "@derive"
        annotations:
          - { name: plumber:derive, args: ["MacroDerived"] }
          - { name: plumber:output, args: ['{{ suffixed "generated" }}'] }

  mixins:
    - plumber.mixin:
        name: mixing.model.filtrable
        annotations:
          - { name: plumber:filter, args: [annotation.has, "is:filtrable"] }
          - { name: plumber:field_wrapper, args: [model.filter] }

  type:
    wrappers:
      - plumber.wrapper:
          name: model.filter
          expressions:
            - plumber.wrapper_expression:
                type: '"github.com/example/contract".Filtrable'
                matches:
                  - rule: 'fqn:"time".Time'
                  - rule: 'kind:interface'
```

### Config hierarchy

`includes` expands globs and merges by appending: sources, templates, macros, mixins,
wrappers. Git sources can declare their own `includes` for co-located config.

Template sources can also be defined at root level under `plumber.templates:` — these are
shared across all commands (shape, discovery). Shape-specific sources under `plumber.shape.sources`
and `plumber.shape.templates.content` are automatically promoted to the root level at load time.

## Macros vs mixins

| | Macros | Mixins |
|---|---|---|
| Source syntax | `@<name>` | `plumber:mixin <name>` |
| Expansion stage | Before transformer building | During transformer building |
| Can inject entry-point annotations | Yes | No (modifier annotations only) |
| Config key | `macros` | `mixins` |

### Macros

Referenced with `@<name>` in source comments. Expand **before** transformers are built,
so they can inject any annotation including `plumber:derive` and `plumber:shape`.

Annotations produced by macros (and mixins) support Go `text/template` with
`.Source.Args`, `.Source.NamedArgs`, `.Package.Name`, and `.Package.Path`.
Templates are evaluated lazily in the transformer stage on a per-annotation
basis: only annotations carrying an `ImpliedBy` reference (i.e. those produced
by a macro or mixin) are template-expanded, which means the same template
context works uniformly for both macros and mixins.

```go
// @tderive Widget file=generated.go
type Order struct { ... }
```

With macro config:
```yaml
- plumber.macro:
    name: "@tderive"
    annotations:
      - { name: plumber:derive, args: ["{{ index .Source.Args 0 }}"] }
      - { name: plumber:output, args: ["{{ .Source.NamedArgs.file }}"] }
```

### Mixins

Referenced with `plumber:mixin <name>`. Expand **during** transformer building. Can only
inject modifier annotations.

```go
// plumber:derive
// plumber:mixin mixing.model.filtrable
type Worker struct { ... }
```

## Queries

The `plumber:query` annotation populates a slice variable with entities matching a regex
pattern. Queries run after template rendering and modify the source file in-place.

```
plumber:query "<regex>" scope="<scope>" [receiver="<var>"]
```

### Scope values

| Scope | Searches |
|---|---|
| `"."` | Current package |
| `".TypeName"` | Fields/methods of a type in current package |
| `"./relpath"` | Relative package |
| `"github.com/pkg"` | External package |
| `"github.com/pkg.TypeName"` | Type in external package |

### Package-level variable

```go
// plumber:query "^Init.*" scope="."
var InitFunctions = []func(){}
```

Populates `InitFunctions` with all exported `func()` matching `^Init.*` in the current
package.

### Function-body variable

```go
func Setup() {
    // plumber:query "^Init.*" scope="."
    var initFuncs = []func(){}
    for _, f := range initFuncs { f() }
}
```

Only explicit `var` declarations with composite literals are supported (not `:=`).

### Type-scoped query

```go
var r Registry
// plumber:query "^Get.*" scope=".Registry" receiver="r"
var Getters = []func() string{}
```

Populates with `r.GetAlpha`, `r.GetBeta`, etc.

## Key rules for agents

- **Do not edit outside `plumber::Block` fences** in generated files — changes will be lost.
- **Use `{{ suffixed "..." }}` for output filenames** to avoid overwriting source files.
- **Macros for entry-point injection**, mixins for modifier bundles — do not confuse them.
- **Queries require explicit `var` with composite literal** — short declarations (`:=`) are not supported.
- **Re-run shape after adding/modifying annotations** to regenerate output.
