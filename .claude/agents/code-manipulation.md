---
name: code-manipulation
description: >
  Expert agent for the plumber `shape` and `inspect` code-manipulation pipeline.
  Use this agent when working on annotation-driven Go code generation, AST inspection,
  YAML config hierarchies, transformer logic, mixin/wrapper configuration, or the
  acceptance test suite under test/acceptance.
---

# Code Manipulation Agent — plumber shape & inspect

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
    ├── config.go               — Config, ShapeConfig, MixinConfig, WrapperConfig, IncludeConfig, …
    ├── contract/contract.go    — annotation name constants + template config types
    ├── shape.go                — Run(): scan → inspect → walk annotations → build transformers → render → restore
    ├── transformer.go          — Transformer interface; DeriveTransformer, ShapeTransformer, BasicTransformer
    ├── manager.go              — GeneratorManager (generated mode), InplaceManager (inplace mode)
    ├── merge.go                — inplace struct field merging via DST
    ├── wrapper.go              — TypeWrapper: rewrites field types by FQN/kind matching
    └── templates/              — git checkout + local load helpers for external templates

internal/astx/inspect/         — ScanFiles(), Inspect(), Walk(), WithAnnotations() — shared AST layer
internal/render/                — render.Context, Output, Derive(), Shape(), Finalize()
query/model/model.go            — Package, Type, Annotation, TypeSpec, Struct, Interface, Function, …

test/acceptance/
├── acceptance.go               — withFixture() helper + AssertContent() golden-file comparison
├── generated_test.go           — TestGenerated: generated mode + mixin filter
└── merge_test.go               — TestMerge: inplace derive into existing struct
    fixture/
    ├── generated/model.go      — annotated with plumber:derive + plumber:mixin
    ├── merge/model.go          — annotated with plumber:derive + plumber:mode inplace
    └── assert/                 — golden files (*.golden) compared byte-for-byte

docs/
├── shape.md                    — full shape command documentation
└── inspect.md                  — full inspect command documentation

example/
├── plumber.shape.yaml          — root config with includes + plumber.shape section
└── plumber.d/config.yaml       — included config with wrappers and mixins
```

---

## Annotation system

Annotations are Go doc-comments in the form `// plumber:<option> [args...]`.

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
  templates:
    sources:
      - local:  { path, templates[] }
      - git:    { repository, ref, templates[] }
    content:
      - { name, content }
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

Merge strategy (in `Config.Merge`): included configs are **appended** to the root — no
key is overwritten.  Append order follows glob expansion order.

---

## Key data-flow (shape)

```
CLI args + config
        │
        ▼
inspect.ScanFiles() → filenames
        │
        ▼
inspect.Inspect()   → []*model.Package   (AST → model)
        │
        ▼
inspect.Walk()       — filter nodes with plumber:shape or plumber:derive annotations
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

---

## Adding a new acceptance test

1. Create fixture source files under `test/acceptance/fixture/<scenario>/`.
2. Add a golden file under `test/acceptance/fixture/assert/<scenario>/<file>.golden`.
3. Write the test in a new `*_test.go` file in `test/acceptance/` following the pattern
   above.
4. Run `go test ./test/acceptance/...` to verify.

---

## Common tasks

| Task | Where to look |
|---|---|
| Add a new annotation option | `contract/contract.go` (add constant) + `transformer.go` (`defaultOptions` slice + `BasicTransformer.Add`) |
| Add a new config field | `config.go` (add field + yaml tag) + `Config.Merge()` if it needs include-merging |
| Add a new filter function | `internal/astx/inspect/` filter predicates |
| Add a new template | `internal/render/templates/` (embedded) or via `plumber.shape.yaml` template sources |
| Debug generation output | Add `plumber inspect ./...` first to see the model that shape will operate on |
