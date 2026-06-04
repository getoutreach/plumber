---
name: or-plumber-shape
description: "Overview of plumber's shape command for annotation-driven code generation — entry-point annotations (derive, shape, query), package-level `plumber:context`, modes, configuration, and pointers to the per-feature skills (derive, shape, query, annotations, functions, structure)."
---

# Shape — Annotation-Driven Code Generation

The `shape` command scans Go packages for `plumber:*` comment
annotations and runs the matching transformations — deriving filtered
structs, rendering Go templates, populating slices from pattern-matched
entities, or merging generated content into existing files.

This skill is the **entry-point overview**. Each entry-point annotation
has its own deep-dive skill:

| Skill | Covers |
|---|---|
| `or-plumber-shape-derive` | `plumber:derive` — projecting a filtered subset struct, with `generated` vs `inplace` modes, idempotent merge mechanics, filters, field wrappers. |
| `or-plumber-shape-shape` | `plumber:shape` — template-driven rendering for structs and interfaces, scopes, multi-template output. |
| `or-plumber-shape-query` | `plumber:query` — populating slice variables with regex-matched functions/methods/fields. |
| `or-plumber-shape-annotations` | Project-resolved catalog of registered options (`plumber:<name>`) and macros (`@<name>`) with schemas. |
| `or-plumber-shape-functions` | Template helper functions available during rendering and macro/mixin expansion. |
| `or-plumber-structure` | Resolved structure paths and the `structure:<name>` references emitted by options/macros. |

## When to use

- You are about to add or modify a `plumber:*` annotation in source and
  need to know which entry point fits the task.
- You need the cross-cutting CLI surface (`shape`, `shape target`,
  `shape describe ...`) and the configuration shape.
- You are deciding between annotating the type directly versus using a
  package-level `plumber:context` group.

## CLI

### Running using mise (preferred when project is managed by mise)

```bash
mise exec -- plumber shape [--config plumber.shape.yaml] [file[:line] ...]
```

### Running using remote path

```bash
go run github.com/getoutreach/outreach/plumber@latest/cmd/plumber \
  shape [--config plumber.shape.yaml] [file[:line] ...]
```

Positional arguments are optional file paths with optional line numbers
for filtering transformations. When omitted, all transformations from
`workingDirs` (or `./...` by default) are processed.

| Flag | Alias | Default | Description |
|---|---|---|---|
| `--config` | `-c` | — | Path to `plumber.shape.yaml`. Lives on the `shape` command — must come **before** any subcommand. |
| `--interactive` | `-i` | `false` | Enable interactive TUI reporter. |

### File targeting

```bash
# Only transformations in model.go
plumber shape ./internal/pkg/model.go

# Only the transformation at/above line 30
plumber shape ./internal/pkg/model.go:30

# Multiple targets
plumber shape ./pkg/a.go:15 ./pkg/b.go
```

**Line matching rules:**

| Line points to | Behaviour |
|---|---|
| An entry-point annotation (`plumber:derive`, `plumber:shape`, `plumber:query`, `plumber:render`) | Only that specific transformation runs |
| A modifier annotation (`plumber:template`, `plumber:filter`, …) | The nearest entry-point annotation **above** is selected |
| The type / declaration itself or a non-annotation doc line | All transformations on that node run |
| A line outside any annotated node | Error |

Standard Go package patterns are configured via the `workingDirs`
property in config rather than passed as CLI arguments.

### Single-type mode

When `--type` and `--macro` are both set, the command skips annotation
scanning and processes only the named type with the named macro:

```bash
plumber shape target \
  --config plumber.shape.yaml \
  --type Worker \
  --macro '@derive' \
  --macro-arg DerivedName \
  --macro-named-arg mode=inplace
```

The macro must exist in `plumber.shape.macros` (see
`or-plumber-shape-annotations`). The type accepts a full FQN
(`"github.com/pkg".Type`) or an unqualified name (`Type`).

## Entry-point annotations at a glance

Each annotation below has a dedicated skill. Read this section to pick
the right one; read the linked skill for the full reference.

### `plumber:derive` — filtered subset structs

Mechanically derives a new struct containing a **subset of the source
struct's fields**, optionally wrapped, renamed, or merged into an
existing type via `plumber:mode inplace`. Accepts struct types only.

```go
// plumber:derive ModelFilter
// plumber:filter annotation.has is:filtrable
// plumber:output {{ filename_suffixed "filter" }}
type Model struct { ... }
```

Deep-dive — including the inplace merge semantics, filters, field
wrappers, and renaming — in the `or-plumber-shape-derive` skill.

### `plumber:shape` — template-driven rendering

Hands the annotated struct **or interface** to one or more Go
`text/template`s and writes the rendered output. Use this when codegen
is fully programmable (converters, registries, gRPC stubs, …) or when
you need interface metadata that `plumber:derive` cannot provide.

```go
// plumber:shape Converter
// plumber:template converter
// plumber:output {{ filename_suffixed "converter" }}
type OrderModel struct { ... }
```

Deep-dive — including templates, scopes, multi-template output, and
notifications — in the `or-plumber-shape-shape` skill.

### `plumber:query` — slice population from regex matches

Populates an annotated `var ... = []T{}` slice with entities (functions,
methods, fields) matching a regex pattern within a configurable scope.
Rewrites the slice literal in place, idempotently.

```go
// plumber:query "^Init.*" scope="."
var InitFunctions = []func(){}
```

Deep-dive — including scope syntax, type-scoped queries, and external
packages — in the `or-plumber-shape-query` skill.

## Modifier annotations

`plumber:*` annotations placed alongside an entry-point refine its
behaviour (`plumber:template`, `plumber:filter`, `plumber:mode`,
`plumber:scope`, `plumber:depends_on`, `plumber:notify`, …). The full
list is **project-specific**: it is composed of plumber's defaults plus
options declared via `plumber.shape.options` and `includes`.

Use the **`or-plumber-shape-annotations`** skill to inspect the
catalog (option names, YAML schemas, registered macros). Do not rely on
memory — registered annotations vary by project.

## Detached annotations via `plumber:context`

By default, an entry-point annotation block lives on the source type's
doc-comment. You can also declare it from **any free-floating comment
group** in any `.go` file of the package — the package doc-comment, a
detached block between two declarations, or a comment group above an
unrelated function — by pairing it with `plumber:context`. The
annotation block is applied to the type referenced by `plumber:context`,
leaving the target type's own doc-comment untouched.

```go
package contract

// some unrelated declaration above ...

// plumber:context "github.com/example/contract".Worker
// plumber:derive WorkerFilterBlended
// plumber:mode inplace

// plumber:context "github.com/example/contract".Worker
// plumber:shape WorkerStub
// plumber:template worker_stub
// plumber:output {{ filename_suffixed "stub" }}

// NewWorker constructs a Worker.
func NewWorker(...) *Worker { ... }
```

Plumber scans every detached comment group in the package — only groups
that are **not attached to a declaration's doc-comment** are eligible.
Place the block on a blank line above or below the surrounding code so
Go's parser treats it as free-floating.

`plumber:context` supports two forms:

| Form | Effect |
|---|---|
| `plumber:context "pkg/path".Type` | **Single-type** — the annotation block applies to one specific type. |
| `plumber:context pkg/path matcher=<name>` | **Package + matcher** — the block applies to every type in the package matched by the named matcher (see `plumber.shape.matchers` in config). |

Use `plumber:context` when:

- The target type lives in a third-party / generated file you cannot
  edit.
- The model type's doc-comment must stay free of codegen directives.
- You want one comment group to fan out across many types in a package
  via a matcher.

Each `plumber:context` block is parsed independently — multiple
context groups can coexist in the same file (or across files in the
same package).

The schema of `plumber:context` (positional argument, `matcher=` named
arg) is documented in `or-plumber-shape-annotations`.

## Modes

`plumber:mode` controls how output is written for `plumber:derive` and
`plumber:shape`. Both annotations share the same modes:

| Mode | Behaviour |
|---|---|
| `generated` (default) | Writes a new file with a `// Generated file...` header. Re-running fully overwrites everything outside `plumber::Block(...)` fences. |
| `inplace` | Idempotently merges generated declarations into an existing file. Adds missing fields/methods/vars; preserves existing ones. Conservative — never deletes user content. |

Inplace merge mechanics (matching keys, deep merge of call args /
composite literals / switch cases) are documented in the
`or-plumber-shape-derive` skill — they apply identically to
`plumber:shape`.

**Do not edit lines outside `plumber::Block` fences** in generated
files — they will be overwritten on re-generation.

## Output filename templates

`plumber:output` is rendered as a Go `text/template`. Plain values
without `{{` are returned verbatim.

| Expression | Expands to |
|---|---|
| `{{ .Filename }}` | Full base filename, e.g. `model.go` |
| `{{ .Name }}` | Filename without extension, e.g. `model` |
| `{{ .Ext }}` | Extension including dot, e.g. `.go` |
| `{{ filename_suffixed "str" }}` | `<.Name>_str<.Ext>` |

Always use `{{ filename_suffixed "..." }}` (or another distinct path)
for `generated` mode — otherwise the source file is overwritten.

## Configuration (`plumber.shape.yaml`)

```yaml
includes:
  - path: plumber.d/*.yaml

plumber.shape:
  workingDirs:          # directories to scan (default: ["./..."])
    - ./internal/...
    - ./pkg/...

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
          - { name: plumber:output, args: ['{{ filename_suffixed "generated" }}'] }

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

  handlers:
    - plumber.handler:
        name: goverter
        command: "goverter gen {{ .Source.NamedArgs.path | join \" \" }}"
```

### Config hierarchy

`includes` expands globs and merges by appending: sources, templates,
macros, mixins, wrappers, handlers. Git sources can declare their own
`includes` for co-located config.

Template sources can also be defined at root level under
`plumber.templates:` — those are shared across all commands. Shape-only
sources under `plumber.shape.sources` and
`plumber.shape.templates.content` are promoted to the root level at
load time.

## Macros vs mixins

| | Macros | Mixins |
|---|---|---|
| Source syntax | `@<name>` | `plumber:mixin <name>` |
| Expansion stage | Before transformer building | During transformer building |
| Can inject entry-point annotations | Yes | No (modifier annotations only) |
| Config key | `macros` | `mixins` |

### Macros

Referenced with `@<name>` in source comments. Expand **before**
transformers are built, so they can inject any annotation including
`plumber:derive` and `plumber:shape`.

Annotations produced by macros and mixins support Go `text/template`
with `.Source.Args`, `.Source.NamedArgs`, `.Package.Name`,
`.Package.Path`, and `.Type`. Templates are evaluated lazily per
annotation in the transformer stage; only annotations carrying an
`ImpliedBy` reference (i.e. those produced by a macro or mixin) are
template-expanded, so the same template context works uniformly for
both.

```go
// @tderive Widget file=generated.go
type Order struct { ... }
```

```yaml
- plumber.macro:
    name: "@tderive"
    annotations:
      - { name: plumber:derive, args: ["{{ index .Source.Args 0 }}"] }
      - { name: plumber:output, args: ["{{ .Source.NamedArgs.file }}"] }
      - { name: plumber:comment, args: ["derived from {{ .Type.Name }}"] }
```

### Mixins

Referenced with `plumber:mixin <name>`. Expand **during** transformer
building. Can only inject modifier annotations.

```go
// plumber:derive
// plumber:mixin mixing.model.filtrable
type Worker struct { ... }
```

The catalog of registered macros and mixins is project-specific — list
it via `or-plumber-shape-annotations`.

## Notifications and handlers

`plumber:notify` triggers a named handler command at the end of the
shape run. This is useful for running post-generation tools (e.g.
`goverter`, `protoc`) on generated output.

```go
// plumber:shape
// plumber:template converter
// plumber:notify goverter path="internal/converters"
type Converter struct { ... }
```

The first positional argument is the handler name. Named arguments are
aggregated across all `plumber:notify` annotations targeting the same
handler.

```yaml
plumber.shape:
  handlers:
    - plumber.handler:
        name: goverter
        command: "goverter gen {{ .Source.NamedArgs.path | join \" \" }}"
```

The command is a Go `text/template` with `.Source.NamedArgs`
(`map[string][]string`). Sprig and plumber generic helpers are
available. Commands execute via `sh -c`; failures fail the shape run.

## Key rules for agents

- **Pick the right entry-point annotation:** `plumber:derive` for
  filtered struct projections, `plumber:shape` for template-driven
  rendering, `plumber:query` for slice population. Each has a dedicated
  skill — load it before writing annotations.
- **Discover modifier annotations and macros via
  `or-plumber-shape-annotations`** — never rely on memory; the catalog
  is project-specific.
- **Use `plumber:context` in a free-floating comment group** when
  annotations must live outside the target type's doc-comment, or when
  applying a bundle to many types via a configured matcher.
- **`plumber::Block(...)` fences** mark hand-written islands in
  generated files; everything outside is rewritten.
- **Use `{{ filename_suffixed "..." }}`** for `plumber:output` in
  `generated` mode to avoid overwriting source files.
- **Macros for entry-point injection**, mixins for modifier bundles —
  do not confuse them.
- **Re-run shape after adding or modifying annotations** to regenerate
  or refresh in-place output.
