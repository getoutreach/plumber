---
name: code-manipulation
description: >
  Expert agent for the plumber `shape` and `inspect` code-manipulation pipeline.
  Use this agent when working on annotation-driven Go code generation, AST inspection,
  YAML config hierarchies, transformer logic, mixin/wrapper configuration, or the
  acceptance test suite under test/acceptance.
---

# Code Manipulation Agent — plumber shape & inspect

> **Sync note:** The user-facing documentation for the shape command lives at
> `docs/shape.md`.  When making changes to the annotation system, YAML config
> structure, macros, mixins, generation modes, or output placeholders described
> in this file, **also update `docs/shape.md`** to keep the two documents in
> sync.  This file contains internal implementation details (data-flow, code
> paths, file locations, acceptance tests); `docs/shape.md` contains user-facing
> usage, examples, and configuration reference.  Acceptance tests and internal
> code paths should **not** be included in `docs/shape.md`.

## Feature overview

Plumber provides two CLI commands that operate on Go source code via AST inspection and
annotation-driven code generation.

| Command | Purpose |
|---|---|
| `plumber inspect` | Scan packages; emit structured JSON/YAML type information |
| `plumber shape`   | Read annotations on types; generate or merge Go code |

Both commands accept a `--config` / `-c` flag pointing at a `plumber.shape.yaml` file and
a package pattern argument (`./...`, `./pkg/...`, etc.).

---

## Repository layout (code-manipulation surface)

```
cmd/plumber/
├── plumber.go                  — CLI entry point, registers all subcommands
├── inspect/inspect.go          — inspect subcommand wiring (flag parsing → internal runner)
└── shape/shape.go              — shape  subcommand wiring (flag parsing, includes merge → runner)

internal/command/
├── command.go                  — shared ParseConfig / ParseConfigs helpers
├── inspect/
│   ├── config.go               — InspectConfig, Config structs
│   └── inspect.go              — Run(): scan → inspect → format (JSON/YAML)
└── shape/
    ├── config.go               — Config, ShapeConfig, MacroConfig, MixinConfig, WrapperConfig, IncludeConfig, …
    ├── contract/contract.go    — annotation name constants + template/source config types
    ├── shape.go                — Run(): scan → inspect → expandMacros → walk annotations → build transformers → render → restore
    ├── transformer.go          — Transformer interface; DeriveTransformer, ShapeTransformer, BasicTransformer
    ├── manager.go              — GeneratorManager (generated mode), InplaceManager (inplace mode)
    ├── merge.go                — inplace struct field merging via DST
    ├── wrapper.go              — TypeWrapper: rewrites field types by FQN/kind matching
    └── templates/              — git checkout (with includes support) + local load helpers for external templates

internal/astx/inspect/         — ScanFiles(), Inspect(), Walk(), WithAnnotations() — shared AST layer
internal/render/                — render.Context, Output, Derive(), Shape(), Finalize()
query/model/model.go            — Package, Type, Annotation, TypeSpec, Struct, Interface, Function, …

test/acceptance/
├── acceptance.go               — withFixture() helper + AssertContent() golden-file comparison
├── generated_test.go           — TestGenerated: generated mode + mixin filter
├── merge_test.go               — TestMerge: inplace derive into existing struct
└── macro_test.go               — TestMacro: macro expansion into plumber:derive + plumber:output
    fixture/
    ├── generated/model.go      — annotated with plumber:derive + plumber:mixin
    ├── merge/model.go          — annotated with plumber:derive + plumber:mode inplace
    ├── macro/model.go          — annotated with @derive macro
    └── @golden/                — golden files (*.golden) compared byte-for-byte

docs/
├── shape.md                    — full shape command documentation
└── inspect.md                  — full inspect command documentation

example/
├── plumber.shape.yaml          — root config with includes + plumber.shape section (sources, templates, macros)
└── plumber.d/config.yaml       — included config with wrappers, macros, and mixins
```

---

## Annotation system

Annotations are Go doc-comments in the form `// plumber:<option> [args...]`.
There are also **macro annotations** in the form `// @<name>` (see below).

### Entry-point annotations (open a transformation)

| Annotation | Applies to | Effect |
|---|---|---|
| `plumber:derive` | struct | Derive a filtered copy of the struct's fields |
| `plumber:shape`  | struct or interface | Shape the type using a Go template |

### Modifier annotations

| Annotation | Key arg(s) | Effect |
|---|---|---|
| `plumber:name`         | `<Name>` | Override generated type name |
| `plumber:output`       | `<file>` | Output path (supports `{filename}`, `{name}`, `{ext}`, `{suffix:str}`) |
| `plumber:mode`         | `generated`\|`inplace` | Generation mode (default: `generated`) |
| `plumber:template`     | `<name>` | Template to render |
| `plumber:mixin`        | `<mixin-name>` | Expand a named mixin from config |
| `plumber:filter`       | `<fn> [args]` | Field filter, e.g. `annotation.has is:filtrable` |
| `plumber:ignore`       | `<Field>` | Exclude field from output |
| `plumber:field_wrapper`| `<wrapper>` | Wrap field types using a config-defined wrapper |
| `plumber:receiver`     | `<Type>` | Override receiver type for generated methods |
| `plumber:comment`      | `<text>` | Append comment to generated declaration |
| `plumber:context`      | `<pkg/Type>` | Package-level annotation: point at a specific model type |
| `plumber:scope`        | `"<Name>" type="<FQN>"` | Inject a resolved `*model.Type` into `.Scope.Custom.<Name>` |

### Custom scope (`plumber:scope`)

`plumber:scope` resolves a fully-qualified type name from the inspected packages and
injects the resulting `*model.Type` into the template scope under `.Scope.Custom.<Name>`.

**Implementation:** In `runTransformations()` (`manager.go`), after setting `scope["Subject"]`,
all `plumber:scope` annotations on the transformer are collected. For each:
1. `annotation.Args[0]` is the key name
2. `annotation.NamedArgs["type"]` is the FQN string
3. `astx.ParseFQN()` parses it, `model.Packages(pkgs).TypeByFQN()` resolves it
4. The resolved type is placed in `scope["Custom"][name]`

Templates access it as `.Scope.Custom.<Name>` (e.g. `.Scope.Custom.Target.Struct.Fields`).
Multiple `plumber:scope` annotations can appear on the same transformation.

### Mixin annotations

Mixins are reusable bundles of **modifier annotations** that are expanded **inside**
`buildTransformers()` — after a transformer (`plumber:shape` or `plumber:derive`) has
already been created.  This means mixins can only inject annotations that a transformer
accepts (modifier annotations like `plumber:filter`, `plumber:template`, `plumber:output`,
`plumber:field_wrapper`, etc.).  They **cannot** inject entry-point annotations like
`plumber:derive` or `plumber:shape`.

Example Go source:
```go
// plumber:derive
// plumber:name WorkerFilter
// plumber:mixin mixing.model.filtrable
// plumber:output {suffix:generated}
type Worker struct { ... }
```

Example config:
```yaml
mixins:
  - plumber.mixin:
      name: mixing.model.filtrable
      annotations:
        - { name: plumber:filter, args: [annotation.has, is:filtrable] }
        - { name: plumber:field_wrapper, args: [model.filter] }
```

At runtime, when `buildTransformers()` in `shape.go` encounters a `plumber:mixin`
annotation, it:

1. Looks up the mixin by name in `config.Mixins`.
2. Iterates over the mixin's annotations and calls `lastTransformer.Accepts()` on each
   to verify the current transformer supports the annotation.
3. Adds each mixin annotation to the current transformer via `lastTransformer.Add()`.

Because mixins are expanded inside the transformer-building loop (after an entry-point
annotation has created a transformer), they attach to whichever `plumber:shape` or
`plumber:derive` block they follow.  A single type can reference multiple mixins across
different transformer blocks.

### Macro annotations

Macros use the `@<name>` syntax (e.g., `@derive`, `@macro`) and are defined in the YAML
config under `macros`.  Unlike mixins (which inject annotations *inside* `buildTransformers`
after a transformer is created), macros expand **before** `inspect.Walk` — so they can
inject **any** annotation, including entry-point annotations like `plumber:derive` and
`plumber:shape`.

Example Go source:
```go
// @derive
type Worker struct { ... }
```

Example config:
```yaml
macros:
  - plumber.macro:
      name: "@derive"
      annotations:
        - { name: plumber:derive, args: ["MacroDerived"] }
        - { name: plumber:output, args: ["{suffix:generated}"] }
```

At runtime, `expandMacros()` in `shape.go` replaces the `@derive` annotation with
`plumber:derive MacroDerived` + `plumber:output {suffix:generated}` on the node's
annotation list.  The rest of the pipeline then processes them normally.

**Key differences between macros and mixins:**

| | Macros | Mixins |
|---|---|---|
| Syntax | `@<name>` | `plumber:mixin <name>` |
| Expansion stage | Before `Walk` (early) | Inside `buildTransformers` (late) |
| Can inject entry-point annotations | Yes | No |
| Config key | `macros` | `mixins` |

---

## Generation modes

### `generated` (default)

- Creates a new file (default `generated.go`).
- Adds a `// Generated file by plumber shape function. DON'T edit manually.` header.
- Preserves `// <<plumber::Block(name)>> … // <</plumber::Block>>` fences for manual edits.
- Managed by `GeneratorManager` (`internal/command/shape/manager.go`).

### `inplace`

- Merges derived fields into an **existing** struct in the same package.
- Adds only fields not already present (idempotent).
- Uses `Merge()` in `merge.go` which walks the DST and appends to the target `StructType`.
- Managed by `InplaceManager`.

---

## YAML config hierarchy

```
plumber.shape.yaml          ← root config loaded by --config
  includes:
    - path: plumber.d/*.yaml  ← glob; each matched file is parsed and merged

plumber.shape:              ← ShapeConfig
  workingDir: …
  cacheDir:   …
  sources:
    - local:  { path, templates[] }
    - git:    { repository, ref, includes[], templates[] }
  templates:
    content:
      - { name, content }
  macros:
    - plumber.macro:
        name: <macro-name>          ← e.g. "@derive"
        annotations:
          - { name, args[], namedArgs{} }
  mixins:
    - plumber.mixin:
        name: <mixin-name>
        annotations:
          - { name, args[], namedArgs{} }
  type:
    wrappers:
      - plumber.wrapper:
          name: <wrapper-name>
          expressions:
            - plumber.wrapper_expression:
                type: '<FQN>'
                matches:
                  - rule: 'fqn:<FQN>'   # or kind:<kind>

plumber.inspect:            ← InspectConfig
  format: json|yaml
  annotations:
    - list: []
```

**Sources** are a direct child of `plumber.shape` (not nested under `templates`).
Git sources support an `includes` field: glob patterns pointing to YAML config files
within the checked-out repo that are parsed and merged into the running config
(same semantics as root-level `includes`).

Merge strategy (in `Config.Merge` / `ShapeConfig.MergeShape`): included configs are
**appended** to the root — no key is overwritten.  Sources, templates content, macros,
mixins, and wrappers are all appended.  Append order follows glob expansion order.

---

## Key data-flow (shape)

```
CLI args + config
        │
        ▼
templates.Checkout()  → clone/update git sources, return include file paths
        │
        ▼
parse git include configs → MergeShape() into running ShapeConfig
        │
        ▼
inspect.ScanFiles() → filenames
        │
        ▼
inspect.Inspect()   → []*model.Package   (AST → model)
        │
        ▼
expandMacros()       — replace @<name> annotations with macro-defined annotation lists
        │
        ▼
inspect.Walk()       — filter nodes with plumber:shape or plumber:derive annotations
                        (including those injected by macros)
        │
        ▼
buildTransformers()  — per-node: create DeriveTransformer or ShapeTransformer,
                        resolve plumber:mixin refs from config
        │
        ▼
group by mode, then by output filename
        │
        ├─► GeneratorManager.Render() → render.Finalize() → []Output{Content: []byte}
        │         → astx.NewParser (post-process imports) → restoreOutput() → write file
        │
        └─► InplaceManager.Render() → render.Finalize() → decorator.Parse()
                  → Merge() (merge into existing DST) → restoreOutput() → write file
```

---

## Acceptance test patterns

Tests live in `test/acceptance/` and use embedded fixtures.

```go
// Pattern used by every acceptance test:
err := withFixture(func(ctx FixtureContext) error {
    err := shape.Run(&shape.ShapeConfig{...}, []string{"./..."})
    assert.NilError(t, err)
    ctx.AssertContent(t, "<output-file>", "<golden-file>.golden")
    return nil
}, "<fixture-file-1>", "<fixture-file-2>")
```

`AssertContent` normalises the random temp-dir suffix before comparing against the golden
file, so golden files use `testrun-acceptance/` as a stable placeholder.

### Existing test cases

| Test | Fixture files | Config | Verifies |
|---|---|---|---|
| `TestGenerated` | `generated/model.go`, `generated/types.go` | `mixing.model.filtrable` mixin with `annotation.has is:filtrable` filter | `generated/generated.go` matches golden |
| `TestMerge` | `merge/model.go`, `merge/types.go`, `merge/blended.go` | No extra config (`ShapeConfig{}`) | `merge/blended.go` merged golden |
| `TestMacro` | `macro/model.go` | `@derive` macro expanding to `plumber:derive MacroDerived` + `plumber:output generated.go` | `macro/generated.go` matches golden |

---

## Adding a new acceptance test

1. Create fixture source files under `test/acceptance/fixture/<scenario>/`.
2. Add a golden file under `test/acceptance/fixture/@golden/<scenario>/<file>.golden`.
3. Write the test in a new `*_test.go` file in `test/acceptance/` following the pattern
   above.
4. Run `go test ./test/acceptance/...` to verify.

---

## Common tasks

| Task | Where to look |
|---|---|
| Add a new annotation option | `contract/contract.go` (add constant) + `transformer.go` (`defaultOptions` slice + `BasicTransformer.Add`) |
| Add a new config field | `config.go` (add field + yaml tag) + `ShapeConfig.MergeShape()` if it needs include-merging |
| Add a new macro | `config.go` (`MacroConfig`) + `plumber.shape.yaml` or included YAML (`macros` section); expansion is automatic via `expandMacros()` in `shape.go` |
| Add a new filter function | `internal/astx/inspect/` filter predicates |
| Add a new template | `internal/render/templates/` (embedded) or via `plumber.shape.yaml` template sources |
| Add a new template source | `contract/contract.go` (`PlumberTemplateSourceConfig`) + `templates/templates.go` (`Load`/`Checkout`) |
| Debug generation output | Add `plumber inspect ./...` first to see the model that shape will operate on |
