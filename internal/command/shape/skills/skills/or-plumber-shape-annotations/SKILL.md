---
name: or-plumber-shape-annotations
description: "Catalog of plumber annotations available in the current project — registered macros (`@<name>`) that expand into annotation bundles before transformer build, and registered options (`plumber:<name>`) used directly in source comments to drive shape transformations."
---

# Shape — Annotations: macros & options

Plumber's `shape` command is annotation-driven. Two kinds of annotations
are registered for a project:

- **Options** — the canonical `plumber:<name>` directives written in Go
  doc-comments (entry-point and modifier annotations). Each option can
  declare a YAML schema for its arguments and an optional `structure:`
  routing target.
- **Macros** — bundles of annotations referenced as `@<name>` in source
  comments. Macros expand **before** the transformer is built, so a single
  `@<name>` can inject any combination of `plumber:derive`,
  `plumber:shape`, `plumber:output`, etc., with template-expanded
  arguments derived from `.Source.Args`, `.Source.NamedArgs`, `.Type`, …

The full inventory — including options/macros pulled in via `includes`,
git sources, and inline `plumber.shape.options` / `plumber.shape.macros`
declarations — is generated from the resolved configuration so it
reflects the actual project setup.

## When to use

- You are writing or modifying a Go file under shape's `workingDirs` and
  need to confirm which `plumber:<name>` directives are accepted.
- You want to know whether a `@<macro>` shorthand exists for a recurring
  annotation pattern (or whether you should add one).
- You need the YAML schema (positional/named arguments, types, defaults)
  for an option before invoking it programmatically — for example through
  `plumber shape target --macro <name> --macro-arg ...`.
- You want a stable JSON/YAML reference for tooling around annotation
  authoring.

## Inspecting registered annotations

### Running using mise (preferred when project is managed by mise)

```bash
mise exec -- plumber shape -c plumber.shape.yaml describe [-f md|json|yaml]
```

### Running using remote path

```bash
go run github.com/getoutreach/outreach/plumber@latest/cmd/plumber \
  shape -c plumber.shape.yaml describe [-f md|json|yaml]
```

| Flag | Alias | Default | Description |
|---|---|---|---|
| `--config` | `-c` | `plumber.yml` | Path to `plumber.shape.yaml`. Lives on the `shape` command — must come **before** the subcommand name. |
| `--format` | `-f` | `md` | Output format: `md`, `json`, or `yaml`. |

The base `describe` subcommand emits everything: macros, options, and
handlers. This skill renders only the annotation-related sections
(options + macros) so they are the focus.

## Reading the catalog

For each **option** the catalog shows:

- **Name** — the literal `plumber:<name>` written in source comments.
- **Doc / Usage** — short description and a real annotation snippet.
- **Singular** — whether only the last instance per node is honored.
- **Schema** — declared argument shape (positional + named arguments,
  types, required flag, defaults) when present.
- **Structure** — if the option routes generated output into a
  `structure:<name>` (see the `or-plumber-structure` skill).
- **Handler** — optional UI handler bound to the option.

For each **macro** the catalog shows:

- **Name** — the literal `@<name>` written in source comments.
- **Annotations** — the bundle the macro expands into; argument values
  are Go `text/template` strings evaluated lazily against the macro's
  invocation source (`.Source.Args`, `.Source.NamedArgs`, `.Type`,
  `.Package`).
- **Structure** — optional routing target, same semantics as on options.

## Annotations registered in this project

### Options

[[ describeOptions ]]

### Macros

[[ describeMacros ]]

## Key rules for agents

- **Use this skill, not memory**, to discover what annotations exist.
  Macros and options are project-specific — they are loaded from the
  resolved config (including `includes` and git sources) and vary across
  modules.
- **Macros (`@<name>`) expand before the transformer is built**, so they
  can inject entry-point annotations like `plumber:derive` and
  `plumber:shape`. Mixins (`plumber:mixin <name>`) cannot — they may
  inject only modifier annotations. See the `or-plumber-shape` skill for
  the macro vs mixin distinction.
- **Macro/mixin argument values are Go templates** evaluated against
  `.Source.Args`, `.Source.NamedArgs`, `.Type`, and `.Package`. The
  available helpers are listed in the `or-plumber-shape-functions`
  skill.
- **Options with a declared `structure:`** route generated output into a
  named structure path; resolve names with `plumber shape describe
  structure` (see the `or-plumber-structure` skill).
- **`-c` lives on `shape`**, not on the subcommand: write
  `plumber shape -c plumber.shape.yaml describe`, not
  `plumber shape describe -c plumber.shape.yaml`.
- **Use `-f json|yaml`** when consuming the catalog from another tool;
  schemas are easier to parse from structured output than from markdown.
