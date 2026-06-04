---
name: or-plumber-shape-functions
description: "Catalog of template functions available during plumber shape rendering — generic helpers (Sprig + plumber generic), render functions (module, annotation, identifier helpers), and shape render functions exposed to shape templates and macro/mixin annotations."
---

# Shape — Template functions

The `shape` command renders Go `text/template` content in three places:

- **Templates** referenced by `plumber:template` / `plumber:shape` (shape
  render).
- **Macro and mixin annotation bodies** (per-annotation expansion at
  transformer-build time).
- **Output filename templates** (`plumber:output`).

Each context exposes a different function set. The full catalog — names,
descriptions, parameters, and usage — is generated from the running
binary so it always reflects the version of plumber installed in the
project.

## When to use

- You are writing a new template referenced by `plumber:template` and need
  to know which helpers are available.
- You are authoring a macro or mixin and want to use `.Source.Args`,
  `.Source.NamedArgs`, or another scope value through a function such as
  `module`, `annotation`, or `path_join`.
- You need to confirm whether a Sprig-style helper (`join`, `index`, …) is
  available in a particular context.
- You want a stable JSON/YAML reference of the functions for tooling.

## Inspecting available functions

### Running using mise (preferred when project is managed by mise)

```bash
mise exec -- plumber shape -c plumber.shape.yaml describe functions [-f md|json|yaml]
```

### Running using remote path

```bash
go run github.com/getoutreach/outreach/plumber@latest/cmd/plumber \
  shape -c plumber.shape.yaml describe functions [-f md|json|yaml]
```

| Flag | Alias | Default | Description |
|---|---|---|---|
| `--config` | `-c` | `plumber.yml` | Path to `plumber.shape.yaml`. Lives on the `shape` command — must come **before** the subcommand name. |
| `--format` | `-f` | `md` | Output format: `md`, `json`, or `yaml`. |

The describe output groups functions by category. Each entry shows:

- **Name** — the identifier used in templates (`{{ name ... }}`).
- **Description** — what the function does.
- **Usage** — a real call snippet copy-pastable into a template.
- **Parameters** — argument types where declared.

## Function catalog

[[ describeFunctions ]]

## Key rules for agents

- **Always check this catalog first** before assuming a helper exists. The
  set varies by plumber version and by where the template runs (output
  filename, macro/mixin annotation, shape render).
- **Macro and mixin annotation bodies** are evaluated lazily per
  annotation; `.Source.Args`, `.Source.NamedArgs`, `.Package`, and `.Type`
  are the canonical scope handles — see usage snippets in the catalog.
- **`module "structure:<name>"`** is the supported way to obtain a module
  helper for a structure path; do not hard-code import paths. See the
  `or-plumber-structure` skill for structure references.
- **`-c` lives on `shape`**, not on the subcommand: write
  `plumber shape -c plumber.shape.yaml describe functions`, not
  `plumber shape describe functions -c plumber.shape.yaml`.
- **Use `-f json|yaml`** when consuming the catalog from another tool.
