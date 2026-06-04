---
name: or-plumber-shape-query
description: "Detailed guide for plumber's `plumber:query` annotation — populating an annotated slice variable with package functions, type methods, or fields whose names match a regex within a configurable scope."
---

# Shape — `plumber:query`

`plumber:query` populates an annotated slice variable with **entities
matching a regex pattern** in a configured scope. Queries run after
template rendering and modify the source file **in place** — they
rewrite the `var` declaration's composite literal so it lists the
matched entities.

This skill is the deep-dive for `plumber:query`. For a high-level
overview of the `shape` command see the `or-plumber-shape` skill. For
the registered options/macros and their schemas see
`or-plumber-shape-annotations`. For template helpers see
`or-plumber-shape-functions`.

## When to use

- Auto-registering all `Init*` functions in a package into a startup
  list.
- Building a slice of constructors / handlers / migrations matching a
  naming convention without hand-maintenance.
- Listing all `Get*` methods of a type for a registry, dispatcher, or
  test harness.
- Aggregating hooks scattered across multiple files of a package.

## Annotation form

```
plumber:query "<regex>" scope="<scope>" [receiver="<var>"]
```

| Argument | Meaning |
|---|---|
| **positional** | Go regex pattern. Matched against entity names within the resolved scope. |
| `scope=` | Where to search (see table below). **Required.** |
| `receiver=` | Variable name used to qualify field/method access (e.g. `r` produces `r.MethodName`). Required when the scope resolves to a named type. |

### Scope values

| Scope | Searches |
|---|---|
| `"."` | The current package |
| `".TypeName"` | Fields/methods of `TypeName` in the current package |
| `"./relpath"` | A relative package |
| `"github.com/pkg"` | An external package |
| `"github.com/pkg.TypeName"` | A type in an external package |

The scope must resolve to packages reachable from the project's
`workingDirs` (or to a referenced external module). External packages
are resolved through the same import resolution shape uses for
`plumber:depends_on` and `plumber:scope type=`.

## Target variables

`plumber:query` only rewrites **explicit `var` declarations whose
right-hand side is a composite literal**. Short declarations (`:=`) and
function-call initialisers are not supported.

### Package-level variable

```go
// plumber:query "^Init.*" scope="."
var InitFunctions = []func(){}
```

After running shape, the literal is populated with every exported
`func()` in the package whose name matches `^Init.*`:

```go
// plumber:query "^Init.*" scope="."
var InitFunctions = []func(){
    InitDB,
    InitCache,
    InitMetrics,
}
```

The generated entries are written **inside** the existing literal — the
annotation is preserved and re-running the query refreshes the slice.

### Function-body variable

```go
func Setup() {
    // plumber:query "^Init.*" scope="."
    var initFuncs = []func(){}
    for _, f := range initFuncs {
        f()
    }
}
```

The same rule applies inside function bodies, with one constraint: the
variable must be declared with the `var` keyword and a composite literal
initialiser. `initFuncs := []func(){}` is **not** supported.

### Type-scoped query

```go
var r Registry

// plumber:query "^Get.*" scope=".Registry" receiver="r"
var Getters = []func() string{}
```

After:

```go
var Getters = []func() string{
    r.GetAlpha,
    r.GetBeta,
    r.GetGamma,
}
```

The `receiver` argument prefixes each match — required for type-scoped
queries because the methods/fields must be invoked through a value of
that type.

### External-package query

```go
// plumber:query "^New.*" scope="github.com/example/widgets"
var Constructors = []any{}
```

External-package scopes match exported entities only (regex still
applies on top). Use these to aggregate constructors from a sibling
module or from a third-party package.

## Compatibility filtering

A regex match alone is not sufficient — the matched entity's signature
must also be **assignable to the slice element type**.

- For `[]func() error`, only zero-arg functions returning `error`
  qualify.
- For a typed function alias such as `[]InitFunc`, the entity must match
  that alias's underlying signature.
- For `[]any` (or `[]interface{}`), any entity whose value can be
  converted to the interface qualifies.

Entities matched by the regex but incompatible with the slice element
type are silently skipped — the query never produces a type error in the
generated literal.

## Interaction with other annotations

- `plumber:query` is an **independent entry-point** — it does not pair
  with `plumber:derive` / `plumber:shape` on the same node.
- It runs **after** template rendering, so a query targeting types
  produced by a `plumber:shape` template in the same run will see those
  results (the rendered file is already on disk by then).
- Modifier annotations like `plumber:depends_on` apply normally:

  ```go
  // plumber:query "^Init.*" scope="."
  // plumber:depends_on "github.com/example/internal/bootstrap".Bootstrap
  var InitFunctions = []func(){}
  ```

  When the dependency does not resolve, the query is silently skipped.

## Declaring queries from a detached comment group

A `plumber:query` block can be attached to **any free-floating comment
group** (package doc-comment, a block between declarations, or any
comment group not attached to a declaration) via `plumber:context`
when the matched slice is declared in another file or when you want to
keep the slice's own doc-comment minimal.

```go
package registry

// plumber:context "github.com/example/registry".Globals
// plumber:query "^Get.*" scope=".Globals" receiver="r"

// NewRegistry constructs a Registry.
func NewRegistry() *Registry { ... }
```

Keep a blank line above and below so Go's parser treats the block as
free-floating rather than attaching it to a neighbouring declaration.

Note: the **target slice** still needs its own annotated `var`
declaration in the package; `plumber:context` only relocates the
annotation block. See `or-plumber-shape` for an overview of context
groups and `or-plumber-shape-annotations` for the
`plumber:context` schema.

## Re-running

Re-running shape on a file containing `plumber:query` rewrites the
literal in place to reflect the current set of matches. Adding a new
function whose name matches the regex appends it; removing a function
removes it. The order of generated entries is deterministic
(lexicographic by name).

## Key rules for agents

- **Use explicit `var ... = []T{}`** declarations. Short declarations
  (`:=`) and function-call initialisers are never rewritten.
- **`scope=` is required**, and the scope syntax distinguishes "current
  package" (`.`), "type in current package" (`.TypeName`), relative,
  absolute, and absolute-with-type forms.
- **`receiver=` is required for type-scoped queries** — entries are
  prefixed with the receiver to call methods correctly.
- **Regex matches names; type compatibility is the second filter.**
  Mismatched-signature matches are dropped silently.
- **Queries run after template rendering**, so they can include
  declarations produced by `plumber:shape` in the same run.
- **Use `plumber:context` in a free-floating comment group** when the
  host file's doc-comment must stay clean or when annotating
  third-party / generated code.
- **Discover allowed query options via `or-plumber-shape-annotations`** —
  the option schema is the source of truth for accepted arguments.
