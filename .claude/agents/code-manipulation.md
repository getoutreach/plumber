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
    ├── shape.go                — Run(): scan → inspect → expand.Macros → walk annotations → build transformers → render → restore
    ├── transformer.go          — Transformer interface; DeriveTransformer, ShapeTransformer, BasicTransformer
    ├── manager.go              — GeneratorManager (generated mode), InplaceManager (inplace mode)
    ├── merge.go                — inplace merge dispatcher: routes TypeSpec, FuncDecl, ValueSpec
    ├── merge_func.go           — function/method merge: params, body, receiver lookup
    ├── merge_stmt.go           — statement-by-statement body merge, deep merge (calls, composite lits, switch cases)
    ├── merge_var.go            — variable merge (add if missing, skip if exists)
    ├── merge_test.go           — unit tests for all merge logic
    ├── wrapper.go              — TypeWrapper: rewrites field types by FQN/kind matching
    ├── query.go                — plumber:query processor: collectQueryTargets, executeQuery, inflateVariable (package-level + function-body vars)
    └── templates/              — git checkout (with includes support) + local load helpers for external templates

internal/astx/
├── dstutil.go                  — shared DST walking utilities: FindNodes, FindNode, Walk, Visitor, RecursiveVisitor, MatchType, etc.
├── inspect/                    — ScanFiles(), Inspect(), Walk(), WithAnnotations() — shared AST layer
internal/render/                — render.Context, Output, Derive(), Shape(), Finalize()
query/model/model.go            — Package, Type, Annotation, TypeSpec, Struct, Interface, Function, …

test/acceptance/
├── acceptance.go               — withFixture() helper + AssertContent() golden-file comparison
├── generated_test.go           — TestGenerated: generated mode + mixin filter
├── merge_test.go               — TestMerge: inplace derive into existing struct
├── merge_missing_test.go       — TestMergeMissingType: inplace derive when target type does not exist (file is created from `plumber:output`)
├── macro_test.go               — TestMacro, TestMacroTemplate
├── query_test.go               — TestQuery, TestQueryTypeScope, TestQueryCrossPackage, TestQueryLocal
└── fixture/
    ├── generated/model.go      — annotated with plumber:derive + plumber:mixin
    ├── merge/model.go          — annotated with plumber:derive + plumber:mode inplace
    ├── mergemissing/           — inplace merge with no pre-existing target type (file is created via plumber:output)
    ├── mergecomplex/           — complex inplace merge: struct fields, functions, methods, switch cases
    ├── macro/model.go          — annotated with @derive macro
    ├── macrotemplate/model.go  — annotated with @tderive (template macro)
    ├── targeted/model.go       — plain struct for single-type targeted mode test
    ├── query/                  — package-level query fixture
    ├── querytypescope/         — type-scoped query fixture
    ├── querycross/             — cross-package query fixture
    ├── querylocal/             — function-body local var query fixture
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
| `plumber:depends_on`   | `<FQN>` | Skip transformation when FQN does not resolve in inspected packages (silent, no error). May repeat. |
| `plumber:query`        | `"<regex>" scope="<scope>" [receiver="<var>"]` | Populate annotated slice variable with matching entities |

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

### Conditional execution (`plumber:depends_on`)

`plumber:depends_on` declares a hard dependency by FQN. The transformation is silently
skipped (no error, no output, no `contentFunc` call) when **any** referenced type cannot
be resolved in the inspected packages. Multiple annotations may appear on the same
transformation; every dependency must resolve for the transformer to run.

**Implementation:** In `runTransformations()` (`manager.go`), before building the
transformation context, `dependsOnSatisfied(transformer, pkgs)` collects every
`plumber:depends_on` annotation, parses each value with `astx.ParseFQN`, and resolves
it via `model.Packages(pkgs).TypeByFQN`. Returns `false` (skip) on the first unresolved
dependency, `true` when all resolve. Malformed FQNs produce an error.

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

At runtime, `expand.Macros()` in `expand.go` replaces the `@derive` annotation with
`plumber:derive MacroDerived` + `plumber:output {suffix:generated}` on the node's
annotation list.  The rest of the pipeline then processes them normally.

#### Template expansion in macros

Macro annotation `args` and `namedArgs` values support Go `text/template` syntax,
allowing macros to forward arguments from the call site into the expanded annotations.

The template context (`macroTemplateData` in `expand.go`) exposes:

- `.Macro.Args` (`[]string`) — positional arguments from the triggering annotation.
- `.Macro.NamedArgs` (`map[string]string`) — named `key=value` arguments from the triggering annotation.
- `.Package.Name` (`string`) — name of the package whose annotation is being expanded.
- `.Package.Path` (`string`) — import path of that package.

The package fields are sourced from the `*model.Package` argument now threaded through
`expandAnnotations(pkg, anns, macroMap)` from `expand.Macros`. A nil package yields empty
`.Package.Name` / `.Package.Path` strings (no panic).

Example config with templates:
```yaml
macros:
  - plumber.macro:
      name: "@tderive"
      annotations:
        - { name: plumber:derive, args: ["{{ index .Macro.Args 0 }}"] }
        - { name: plumber:output, args: ["{{ .Macro.NamedArgs.file }}"] }
        - { name: plumber:comment, args: ["from {{ .Package.Path }}"] }
```

Example Go source:
```go
// @tderive Widget file=generated.go
type Order struct { ... }
```

This expands to `plumber:derive Widget` + `plumber:output generated.go`.

Implementation details:
- `expandTemplateStr()` short-circuits when the string has no `{{`, so the existing
  `{name}` / `{suffix:...}` transformer placeholders pass through unaffected.
- Template errors (bad syntax, missing keys) cause a hard failure — `expand.Macros()`
  returns an error that aborts the pipeline.
- Only `args` and `namedArgs` values are templated; annotation names are not.

### Query annotations (`plumber:query`)

`plumber:query` annotates a slice variable (package-level or function-body) to populate
it with entities matching a regex pattern.  Queries run as a separate pass in `processQueries()`
**after** macro expansion and before transformer building.

**Syntax:** `// plumber:query "<regex>" scope="<scope>" [receiver="<var>"]`

- First positional arg: regex pattern matched against entity names (function names,
  method names, etc.)
- `scope`: package path to search, or `"."` for same package, or a type FQN like
  `".TypeName"` for type-scoped queries (searches methods/fields of that type)
- `receiver`: required when scope resolves to a named type — the variable name used
  to qualify method calls (e.g., `receiver="r"` → `r.MethodName()`)

**Variable requirements:**
- Must be a typed slice (e.g., `[]func()`) — the element type determines which
  entities are compatible
- Package-level: standard `var` declaration with composite literal
- Function-body: explicit `var` declaration with composite literal (not `:=`)

**Implementation flow** (`query.go`):
1. `collectQueryTargets()` — finds package-level vars via `model.PackageVar` annotations
   and function-body vars via `collectLocalQueryTargets()` (DST walking with `astx.FindNodes`)
2. `parseQueryAnnotation()` — extracts pattern, scope, receiver from the annotation
3. `executeQuery()` — resolves scope, finds matching entities, filters by type compatibility
4. `inflateVariable()` — modifies the DST composite literal to inject matched results
5. `processQueries()` — orchestrates the above, writes modified files back

For function-body variables, annotations are parsed from DST decoration strings
(`GenDecl.Decs.Start`) using `annotationsFromDecs()` which strips `//` prefixes and
delegates to `inspect.ParseAnnotations`. Type resolution uses the DST→AST node mapping
(`pkg.Decorator.Ast.Nodes`) to access `types.Info.Defs`.

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
- If the target type does **not** exist in the package, the generated declaration is
  appended to the file named by `plumber:output` (defaults to `generated.go`). The file
  is created on demand via `findOrCreateOutputFile()` and registered with the package
  decorator so the restorer writes it like any other file.
- `BasicTransformer.Output()` resolves to `plumber:output` (or the default `generated.go`)
  for **both** generated and inplace modes — there is no longer a hardcoded `inplace.go`.
  The intermediate fragment used during template rendering uses its own internal name
  (`plumber_inplace_helper.go`) and is unrelated to the user-facing output.

#### Inplace merge implementation

The merge pipeline is: render template → `decorator.Parse()` → `Merge(pkg, generatedFile, output)`
→ returns modified `*dst.File` → `decorator.NewRestorerWithImports` → write file. The
`output` argument is the per-transformer `Output()` value and is used by `Merge()` to
choose the destination file when a missing type must be appended.

**Source files:**

| File | Contents |
|---|---|
| `merge.go` | `Merge(pkg, file, output)` dispatcher — routes `*dst.TypeSpec`, `*dst.FuncDecl`, `*dst.ValueSpec` to sub-mergers; `addTypeDecl()`/`findOrCreateOutputFile()` append/create files for missing types |
| `merge_func.go` | `mergeFunc()`, `addFunc()`, `mergeParams()`, `findFuncDecl()`, `annotateFuncDecl()`, `findExistingFunc()` |
| `merge_stmt.go` | `mergeBody()`, `statementsMatch()`, `deepMergeStmt()`, `deepMergeExpr()`, `mergeCallArgs()`, `mergeCompositeLit()`, `mergeSwitchCases()`, `exprKey()`, `stmtKey()`, `annotateStmt()` |
| `merge_var.go` | `mergeVar()` — add if missing, skip if exists |
| `merge_test.go` | Unit tests for all merge logic |

**Struct field merge** (`merge.go`): Iterates generated struct fields, checks existing
struct by field name, appends missing fields with `astx.AnnotateFieldIdents()` for import
annotation.

**Function/method merge** (`merge_func.go`):
- `findExistingFunc()` searches both `pkg.Functions` (top-level functions) and
  `type.Struct.Methods` (for method declarations with receivers).
- Match is by **name only**; receiver type is used for lookup scope but variable name is
  ignored.
- `mergeParams()` does positional prefix matching — template params must be a prefix of
  existing params. Missing template params are appended.
- Body merge delegates to `mergeBody()`.

**Body merge** (`merge_stmt.go`):
- Empty existing body: all template statements inserted.
- Non-empty: template statements must appear as an **ordered subsequence**. Each template
  statement is matched via `statementsMatch()` (shallow key) then `deepMergeStmt()`.
  If a template statement is not found, merge returns an error.

**Statement matching keys** (`statementsMatch()`):
- `*dst.AssignStmt` → LHS expressions via `exprKey()`
- `*dst.ExprStmt` (call) → call target function name
- `*dst.ReturnStmt` → always matches
- `*dst.DeclStmt` → first variable name
- `*dst.SwitchStmt` → tag expression via `exprKey()`
- `*dst.IfStmt`, `*dst.ForStmt`, `*dst.RangeStmt` → same Go type

**Deep merge** (`deepMergeStmt()`, `deepMergeExpr()`):
- Recursively walks into RHS, return values, call args, composite lit fields.
- `mergeCallArgs()`: matches args by `exprKey()`, appends missing template args.
- `mergeCompositeLit()`: matches key-value entries by key name, deep-merges values of
  matching entries, appends missing entries.
- `mergeSwitchCases()`: matches case clauses by `caseClauseKey()` (expression values for
  regular cases, `"default"` for default). Missing cases inserted after last matched
  preceding case. Matched case bodies are deep-merged. Existing extra cases preserved.

**Import annotation** (`astx.RewriteExpr()`, `annotateStmt()`):
- The DST decorator resolves `pkg.Func()` into `*dst.Ident{Name: "Func", Path: "pkg/path"}`
  rather than `*dst.SelectorExpr`. New statements cloned from the template need
  `astx.RewriteExpr()` to convert `SelectorExpr` → annotated `Ident` for the restorer.

**Key invariant:** the DST import map (`astx.BuildImportMap(file)`) must be built from
the **generated** file's imports, since template code references packages via their
import aliases in the generated output.

**Acceptance test:** `TestMergeComplex` in `test/acceptance/merge_complex_test.go` exercises
the full pipeline: struct field merge, param merge, composite lit deep merge, body
subsequence matching, call arg augmentation, method lookup, and switch case merge.

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
[if config.Target != nil] → runTargeted():
        │   resolve type by FQN or name → inject macro annotation → expand.Macros
        │   → buildTransformers → renderTransformations → restoreOutputs
        │   (skips full annotation scan, queries, and other types)
        │
        ▼ [else: normal mode]
expand.Macros()       — replace @<name> annotations with macro-defined annotation lists;
                        template expressions ({{ .Macro.Args }}, {{ .Macro.NamedArgs }},
                        {{ .Package.Name }}, {{ .Package.Path }}) in the macro's
                        args/namedArgs are expanded against the call-site arguments and
                        the enclosing package metadata
        │
        ▼
inspect.Walk()       — filter nodes with plumber:shape or plumber:derive annotations
                        (including those injected by macros)
        │
        ▼
processQueries()     — find plumber:query-annotated variables (package-level + function-body),
                        match entities by regex/scope, inflate composite literals via DST
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
| `TestMergeMissingType` | `mergemissing/model.go`, `mergemissing/types.go` (no pre-existing target type) | No extra config | `mergemissing/merged.go` is created from scratch — verifies inplace mode appends a synthesized type to the file named by `plumber:output` when the target type is absent |
| `TestMergeComplex` | `mergecomplex/model.go`, `mergecomplex/types.go`, `mergecomplex/blended.go` | Content template override | Full merge pipeline: struct fields, params, body subsequence, call arg augmentation, composite lit merge, method lookup, switch case merge |
| `TestMacro` | `macro/model.go` | `@derive` macro expanding to `plumber:derive MacroDerived` + `plumber:output generated.go` | `macro/generated.go` matches golden |
| `TestMacroTemplate` | `macrotemplate/model.go` | `@tderive` macro with `{{ index .Macro.Args 0 }}` template expanding call-site arg into derive name | `macrotemplate/generated.go` matches golden |
| `TestShapeTargeted` | `targeted/model.go` | `@derive` macro with template arg; `TargetConfig` set (single-type mode) | `targeted/generated.go` matches golden; also tests macro-not-found and type-not-found errors |
| `TestQuery` | `query/providers.go`, `query/consumer.go` | No extra config | `query/consumer.go` inplace with matched provider functions |
| `TestQueryTypeScope` | `querytypescope/types.go`, `querytypescope/consumer.go` | No extra config | `querytypescope/consumer.go` inplace with matched type methods |
| `TestQueryCrossPackage` | `querycross/providers/providers.go`, `querycross/consumer.go` | No extra config | `querycross/consumer.go` inplace with cross-package matches |
| `TestQueryLocal` | `querylocal/providers.go`, `querylocal/consumer.go` | No extra config | `querylocal/consumer.go` inplace with function-body var populated |

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
| Add a new macro | `config.go` (`MacroConfig`) + `plumber.shape.yaml` or included YAML (`macros` section); expansion is automatic via `expand.Macros()` in `expand.go`. Macro args/namedArgs values support `text/template` syntax (see `macroTemplateData` in `expand.go`) |
| Add a new filter function | `internal/astx/inspect/` filter predicates |
| Add a new template | `internal/render/templates/` (embedded) or via `plumber.shape.yaml` template sources |
| Add a new template source | `contract/contract.go` (`PlumberTemplateSourceConfig`) + `templates/templates.go` (`Load`/`Checkout`) |
| Add a new query pattern | `query.go` (`collectQueryTargets`, `executeQuery`, `inflateVariable`); for new entity types, extend `matchEntity` / type compatibility checks |
| Debug generation output | Add `plumber inspect ./...` first to see the model that shape will operate on |
