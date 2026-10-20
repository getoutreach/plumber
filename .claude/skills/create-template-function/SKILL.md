---
name: create-template-function
description: How to implement, register, document and test a new Go text/template function exposed to plumber templates (shape render, generic render helpers, or annotation value expansion), following the fields_match / filter_elements pattern.
---

# Creating a new template function

Plumber templates are rendered with Go `text/template`. Functions callable from templates
are defined in Go and registered in a *function catalog*. The catalog is also what
`plumber shape -c plumber.shape.yaml describe functions` prints, and it is embedded into the
shipped `or-plumber-shape-functions` skill via `[[ describeFunctions ]]`, so a function
that is registered but not described is invisible to users and agents.

Reference implementation: `fields_match` in
`internal/command/shape/render/functions_fields.go` (+ `functions_fields_test.go`).

## 1. Pick the catalog

| Where the function must be callable | Catalog file | Context type |
|---|---|---|
| Shape templates (`plumber:template`, `plumber:shape`, derive, finalize) — functions that work on the model being shaped | `internal/command/shape/render/function_descriptions.go` | `*render.EvaluationContext` (wraps shape `*Context`) |
| Any render (shared helpers: `module`, `annotation`, `placeholder`, `extend`, …) | `internal/render/function_desciptions.go` → `FunctionsDescription()` | `*render.EvaluationContext` (Context, Scope, Output, Type) |
| Any render, pure / context-free helpers | `internal/render/function_desciptions.go` → `GenericFunctionsDescription()` | `contract.VoidContext` |
| Annotation value expansion (macro/mixin bodies, `plumber:output` filenames) | `internal/command/shape/expand/function_descriptions.go` | `*expand.EvaluationContext` |

The sections shown by `describe functions` are assembled in
`cmd/plumber/shape/shape_describe.go` (`buildFunctionsDescription`). A new catalog needs a
new entry there; a new function in an existing catalog does not.

## 2. Implement the function

Put non-trivial functions in their own file next to the catalog, e.g.
`internal/command/shape/render/functions_<topic>.go`, with the standard file header
(see the `lint-file-header` skill):

```go
// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the <name> template function, which ...

package render
```

Rules:

- **Unexported Go name in camelCase** (`fieldsMatch`); the template name is **snake_case**
  (`fields_match`).
- **Use concrete parameter types** (`[]*model.Var`, `string`) instead of `any` when the
  inputs are known. `describe` prints parameter/return types via reflection, so concrete
  types document themselves, and `text/template` reports a clear error on mismatch.
  Use `any` + reflection only when the function must accept arbitrary slices
  (see `filterElements`).
- **Return `(T, error)`** when the input can be invalid. `text/template` aborts rendering
  with the error. Prefix messages with the template name:
  `fmt.Errorf("fields_match: unknown method %q ...", method)`.
- **Mode/enum string arguments**: define exported constants, accept them case-insensitively
  and trimmed, give the empty string a sensible default, reject unknown values with an
  error listing the valid ones.
- **Be nil-tolerant**: skip `nil` elements and treat a `nil` slice as empty — model data is
  often partially populated.
- **Be deterministic**: preserve input order; never iterate a map to build output.
- **Pipelines**: `text/template` passes the piped value as the *last* argument
  (`{{ .A | f "x" }}` = `f "x" .A`). Order parameters accordingly if pipe use is expected.
- Document the semantics on the Go function (godoc) — it is the source of truth for
  maintainers; the catalog description is the source of truth for template authors.

## 3. Register and describe

### Shape render catalog (two places — both are required)

1. Add a descriptor to `FunctionsDescription()` in
   `internal/command/shape/render/function_descriptions.go`:

   ```go
   {
       Description: contract.FunctionDescription{
           Name: "fields_match",
           Description: `Return the fields of the first set that have a counterpart by name in the second set, ` +
               `preserving the order of the first set. Method "strict" (default when empty) ...`,
           Usage: `{{ range $f := fields_match $.Type.Struct.Fields $.Scope.Custom.Target.Struct.Fields "tolerant" }}{{ $f.Name }}{{ end }}`,
       },
       Func: func(c *EvaluationContext) any {
           return fieldsMatch // or a closure capturing c.Context when context is needed
       },
   },
   ```

2. Add the same name to the hand-written map in `withRenderFuncMap` in
   `internal/command/shape/render/functions.go`. **This map is what `render_shape.go`,
   `render_derive.go` and `render_finalize.go` actually use at render time** — a function
   only present in `FunctionsDescription()` is documented but fails at render with
   `function "x" not defined`.

### Other catalogs

Add one descriptor to the corresponding `FunctionDescriptors` slice; their `ToMap` /
`WithRenderFuncMap` builders feed both rendering and `describe`.

### Writing the description

- **Description**: what it returns, every argument's meaning, defaults, and edge cases
  (ordering, nil handling, error conditions). Users only see this text.
- **Usage**: a real, copy-pastable call using actual scope paths
  (`$.Type.Struct.Fields`, `$.Scope.Custom.<Name>`, `$.Scope.Subject`) — not placeholders
  like `.Elements`.
- `Func` receives the evaluation context. Return a closure only if you need it
  (e.g. `ignored(c.Context.Ignores)`); otherwise return the plain function.
  `describe` calls `Func` with a zero context to reflect the signature, so `Func` itself
  must not dereference the context — only the returned closure may.

## 4. Test

Create `functions_<topic>_test.go` in the same package:

1. **Table-driven unit tests** of the Go function: happy paths for every mode, defaults,
   case/format edge cases, nil inputs, and the error path for invalid arguments.
2. **Registration test** proving the function is described *and* callable from a template:

   ```go
   desc, build := FunctionsDescription()
   // assert desc.Descriptions() contains the name with non-empty Description and Usage
   fm, dispose := build(&Context{
       ContextCloner: &render.RenderContext{Modules: render.NewModuleRegister()},
       Ignores:       NewIgnores(),
   }, nil, "")
   defer dispose()
   tpl := template.Must(template.New("t").Funcs(fm).Parse(`{{ range $f := fields_match .A .B "tolerant" }}{{ $f.Name }};{{ end }}`))
   ```

   Building the shape render map requires a context with `ContextCloner` and `Ignores`
   set, because other functions (`type_wrap`, `ignored`) capture them.

Run:

```bash
go vet ./internal/command/shape/render/
go test ./internal/command/shape/render/ ./internal/render/ ./internal/command/shape/expand/ -count=1
```

## 5. Regenerate and verify docs

```bash
# verify the entry, parameters and returns
go run ./cmd/plumber/plumber.go shape -c example/plumber.shape.yaml describe functions | sed -n '/### <name>/,/^### /p'

# regenerate the checked-in catalog (run from the example/ directory)
sh docs.sh   # writes example/docs/functions.md
```

`-c` belongs to `shape` and must come **before** `describe`.

The shipped skill `internal/command/shape/skills/skills/or-plumber-shape-functions` renders
the catalog dynamically through `[[ describeFunctions ]]` — do not hand-edit a function
list into it. Update other skills/docs only when they reference the function by name in
prose (e.g. the helper list in `or-plumber-shape-derive/SKILL.md`).

## Checklist

- [ ] Implementation file with copyright + `Description:` header and godoc
- [ ] Concrete parameter types; `(T, error)` for invalid input; nil-tolerant; ordered output
- [ ] Descriptor (Name, Description, real Usage) in the right catalog
- [ ] Shape render only: also added to `withRenderFuncMap` in `functions.go`
- [ ] Unit tests + template registration test
- [ ] `describe functions` shows correct signature; `example/docs/functions.md` regenerated
- [ ] `go vet` and `go test` pass
