---
name: or-plumber-shape-derive
description: "Detailed guide for plumber's `plumber:derive` annotation — generating a filtered subset struct from an existing one, with `generated` vs `inplace` modes, idempotent inplace merge mechanics, field filters, and field wrappers."
---

# Shape — `plumber:derive`

`plumber:derive` is an entry-point annotation that produces a **new struct
type whose fields are a filtered subset** of the annotated struct.
It accepts only struct types (interfaces are rejected).

Typical use cases:

- Deriving an API DTO / projection from a model struct.
- Producing a "filter" or "patch" struct keeping only writable fields.
- Splitting one struct into multiple specialised views, each with its own
  filter predicate.
- Merging a generated subset back into a hand-written struct (`inplace`).

This skill is the deep-dive for `plumber:derive`. For the high-level
overview of the `shape` command see the `or-plumber-shape` skill. For the
list of available modifier annotations and their schemas see
`or-plumber-shape-annotations`. For template helpers callable from
generated output see `or-plumber-shape-functions`.

## When to use

- You have a domain struct and need a parallel type containing a subset
  of its fields (with optional renaming via `plumber:field_wrapper`).
- You want a generated file regenerated on every shape run, or an
  in-place merge that preserves hand-written code.
- You want to declare the derivation from a **detached comment group**
  instead of on the type itself (use `plumber:context` — see below).

## Minimal example — `generated` mode

```go
// plumber:derive ModelFilter
// plumber:filter annotation.has is:filtrable
// plumber:output {{ filename_suffixed "filter" }}
type Model struct {
    // is:filtrable
    Name string

    Concurrency int  // excluded — no is:filtrable annotation
}
```

Produces `model_filter.go`:

```go
// Generated file by plumber shape function. DON'T edit manually.
type ModelFilter struct {
    Name string
    // <<plumber::Block(extra-ModelFilter)>>
    // <</plumber::Block>>
}
```

Lines outside `plumber::Block(...)` fences are overwritten on every run.
Hand-written extensions go inside the block.

## Modes

`plumber:derive` honours `plumber:mode`:

| Mode | Behaviour |
|---|---|
| `generated` (default) | Writes a new file with a `// Generated file...` header. Re-running fully overwrites everything outside `plumber::Block` fences. |
| `inplace` | Idempotently merges derived fields into an existing struct in the configured output file. Adds missing fields, never removes. Preserves field types/tags/comments. |

### `inplace` merge mechanics

Inplace mode is conservative: it adds what is missing without removing
anything the user has written. Repeated runs are no-ops.

**Struct fields:** matched by field name. Missing fields are appended at
the end of the struct; existing fields are preserved (type, tags,
comments).

**Functions / methods on the derived type:** matched by name.

- Missing functions are added entirely.
- Existing functions with empty bodies receive all template statements.
- Existing functions with non-empty bodies require template statements as
  an **ordered subsequence**: if a template statement is missing from the
  existing body, the merge fails. Removed statements are treated as
  intentional user changes.
- Parameters are merged positionally — template params must form a
  prefix; missing trailing params are appended.

**Variables:** matched by name; added when missing, skipped when present.

**Statement matching (shallow key):**

| Statement | Match key |
|---|---|
| Assignment | LHS expression(s) |
| Expression (call) | Call target function name |
| Return | Keyword (always matches) |
| Declaration | Variable name(s) |
| Switch | Tag expression |
| If / For / Range | Same Go statement type |

**Deep merge of matched statements:**

- **Call arguments** — template args must be present; extra existing args
  preserved; missing args appended.
- **Composite literals** — template key/value entries must be present and
  are matched by key name; missing entries appended; recursive at any AST
  depth.
- **Switch cases** — matched by case expression values; missing template
  cases inserted after the last matched preceding case; extra existing
  cases preserved; matched-case bodies are deep-merged.

**Idempotence** — once both sides converge, subsequent runs touch
nothing.

If the target type does not yet exist in the package, the generated
declaration is appended to the file named by `plumber:output` (defaults
to `generated.go`). The file is created on demand, so `inplace` is safe
to use for both initial generation and subsequent merges.

```go
// plumber:derive
// plumber:mode  inplace
// plumber:name  ModelBlended
type Model struct {
    Name   string
    Closer OpenCloser
}
```

## Filters

Use `plumber:filter` to select which source fields are copied into the
derived struct. The filter is a predicate evaluated against each field's
annotations / type metadata.

```go
// plumber:derive ModelFilter
// plumber:filter annotation.has is:filtrable
type Model struct {
    // is:filtrable
    Name string
    Concurrency int  // dropped
}
```

Multiple `plumber:filter` annotations are combined (AND). Use
`plumber:ignore <FieldName>` to exclude a specific field by name even if
the filter would otherwise include it.

The catalog of available filter functions is project-specific; see
`or-plumber-shape-annotations` for the registered options and schemas.

## Field wrappers

`plumber:field_wrapper <name>` applies a configured type wrapper to each
included field — for example wrapping every field type in a generic
`Filter[T]`. Wrappers are declared in `plumber.shape.type.wrappers`:

```yaml
plumber.shape:
  type:
    wrappers:
      - plumber.wrapper:
          name: model.filter
          expressions:
            - plumber.wrapper_expression:
                type: '"github.com/example/contract".Filtrable[{{ .Type }}]'
                matches:
                  - rule: 'kind:basic'
```

Use:

```go
// plumber:derive ModelFilter
// plumber:field_wrapper model.filter
// plumber:filter annotation.has is:filtrable
type Model struct { ... }
```

## Receiver and naming

| Annotation | Purpose |
|---|---|
| `plumber:name <Name>` | Rename the derived type. The first positional argument of `plumber:derive` is shorthand for `plumber:name` when no explicit annotation follows. |
| `plumber:receiver <Type>` | Override receiver type for any methods generated alongside the derived struct. |
| `plumber:comment <text>` | Append a comment line to the generated declaration. |

## Declaring derivations from a detached comment group

`plumber:derive` (and modifiers) are normally placed on the doc comment
of the source type. They can also be declared from **any free-floating
comment group** in any `.go` file of the package by pointing at the
source type with `plumber:context`. This works for the package
doc-comment, comment blocks between declarations, or any comment group
not attached to a declaration. It is useful when the source type lives
in a different file, or when you want to keep generated annotations out
of the model's own doc-comment.

```go
package contract

// some unrelated declaration above ...

// plumber:context "github.com/example/contract".Worker
// plumber:derive WorkerView
// plumber:filter annotation.has is:public
// plumber:output {{ filename_suffixed "view" }}

// NewWorker constructs a Worker.
func NewWorker(...) *Worker { ... }
```

The block must be a **free-floating** comment group — leave a blank
line above or below so Go's parser does not attach it to a neighbouring
declaration.

`plumber:context` accepts:

- A **fully-qualified type name** (`"pkg/path".Type`) — single-type mode.
- A **package import path with a `matcher=<name>` named arg** —
  package-matcher mode, applies the bundle to every type in the package
  matching the named matcher.

See the `or-plumber-shape-annotations` skill for the
`plumber:context` schema, and `or-plumber-shape` for an overview of how
context groups are scanned.

## Output filename templates

`plumber:output` is rendered as a Go `text/template`. Plain values
without `{{` are returned verbatim.

| Expression | Expands to |
|---|---|
| `{{ .Filename }}` | Full base filename, e.g. `model.go` |
| `{{ .Name }}` | Filename without extension, e.g. `model` |
| `{{ .Ext }}` | Extension including dot, e.g. `.go` |
| `{{ filename_suffixed "filter" }}` | `<.Name>_filter<.Ext>`, e.g. `model_filter.go` |

Always use `{{ filename_suffixed "..." }}` (or another distinct filename)
for `generated` mode — writing to the source filename will overwrite the
hand-written struct.

## Targeting a single derivation from the CLI

Single-type mode bypasses annotation scanning and runs a configured
macro against one type:

```bash
plumber shape target \
  --config plumber.shape.yaml \
  --type Worker \
  --macro '@derive' \
  --macro-arg DerivedName \
  --macro-named-arg mode=inplace
```

The `@derive` macro must be declared in `plumber.shape.macros`. See the
`or-plumber-shape-annotations` skill for the registered macros in the
current project and `or-plumber-shape` for the broader CLI surface.

## Key rules for agents

- **`plumber:derive` accepts only structs.** For interfaces or
  template-driven rendering use `plumber:shape` — see the
  `or-plumber-shape-shape` skill.
- **Use `inplace` when the target file is hand-written**, `generated`
  when the file is fully owned by shape. Mixing the two on the same file
  loses hand edits.
- **Always pick a distinct output filename** in `generated` mode (use
  `filename_suffixed`) — otherwise the source file is overwritten.
- **`plumber::Block` fences are the only safe place** for hand-written
  code in generated files; everything outside is rewritten.
- **Inplace is an additive merge.** Removing a field/statement from the
  template requires removing it from the merged file by hand — shape
  will not delete user-owned content.
- **Declare derivations from a detached comment group via
  `plumber:context`** when the source type's doc-comment must stay
  clean or when generating for a third-party type.
- **Discover filters, wrappers, and macros via the annotations skill** —
  they are project-specific and live in the resolved configuration.
