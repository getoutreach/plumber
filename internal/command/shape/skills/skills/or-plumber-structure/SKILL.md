---
name: or-plumber-structure
description: "Provides guidance on plumber's structure system: declaring named project paths in config, inspecting them with `shape describe structure`, and referencing them from templates and annotations via `structure:<name>`."
---

# When to use
- When you need to know what directories the current project has registered
  for code generation. The structure will give guidance for the code placement.
  It provides a stable vocabulary for project paths, so you can discover and reference them.
- Before authoring or maintaining macros, mixins, or templates that emit code
  into a structured layout (e.g. hexagonal architecture).
- When wiring a new annotation that should route generated code into a
  specific layer (`structure: structure:domain.entity` on an option or macro).
- When scripting around plumber: `--format json|yaml` gives a stable inventory
  to consume from other tools.

# Scaffolding project directories

In order to scaffold project directories use following command. Structures that were declared as required will be created.

```bash
plumber shape -c [plumber.yaml] structure
```

# Structure — Named project paths

A plumber **structure** is a named, project-scoped directory layout declared in
`plumber.shape.yaml`. It pairs a base path under the module with a list of named sub-paths
(`domain.entity`, `adapter.outbound.pgsql`, …). Anywhere code generation needs
to reference a project directory — templates, annotations, macros — it does so
by **name**, via the `structure:<name>` reference syntax. Plumber resolves the
name to a concrete filesystem path and import path for the consuming module.

The base path can contain variables (`{{ .Module.NormalizedName }}`) that get expanded at runtime, so the same structure definition can be reused across modules.
The sub-paths are stable identifiers for important project directories, and can be used as generation targets in annotations and macros.

| Variable| Description |
|---|---|
| `{{ .Module.NormalizedName }}` | The consuming module's normalized name, which is the module path with the repo prefix stripped. For example, If the module path is `github.com/getoutreach/some-other-repo`, the normalized name is `some_other_repo`. |

## Inspecting structures

The canonical way to discover structures is the `shape describe structure`
subcommand.

### Running using mise (preferred when project is managed by mise)

```bash
mise exec -- plumber shape [-c plumber.yml] describe structure [-f md|json|yaml]
```

### Running using remote path

```bash
go run github.com/getoutreach/outreach/plumber@latest/cmd/plumber \
  shape [-c plumber.yml] describe structure [-f md|json|yaml]
```

| Flag | Alias | Default | Description |
|---|---|---|---|
| `--config` | `-c` | `plumber.yml` | Path to `plumber.yml`. Lives on the `shape` command — must come **before** the subcommand name. |
| `--format` | `-f` | `md` | Output format: `md`, `json`, or `yaml`. |

## Output anatomy (markdown)

```
## <structure-name>

<title>

**Base path:** `<expanded base path>`

**Documentation:**

<long-form documentation>

### <sub-path title — falls back to name then relative path>

<sub-path long-form documentation>

| Field | Value |
|-------|-------|
| Name | `<sub-path name>` |
| Relative path | `<expanded relative path>` |
| Required | yes \| no |
| Package description | <optional> |
```

Per **structure**:

- Heading is the structure `name` (the identifier you reference in code).
- First paragraph is the structure `title` (short human label).
- `Base path` is the structure `path` after template expansion.
- `Documentation` is the structure's long-form prose, when present.

Per **sub-path**:

- Heading is the sub-path `title`, falling back to `name`, then to the
  relative path.
- The body paragraph is the sub-path `documentation`.
- The table surfaces the canonical metadata an agent needs: identifier,
  resolved relative path, whether the directory is *required* (see
  `plumber shape structure` below), and an optional `Package description`
  used as the package-level doc comment when the sub-path is scaffolded.

JSON / YAML output exposes the same fields plus `usage` (the canonical
`structure:<name>` reference) and the structure's `name` and `documentation`.
Use `-f json` for programmatic consumption.

## The `structure:<name>` reference

Anywhere plumber accepts a structure reference, the syntax is
`structure:<sub-path-name>`. The resolver walks the configured structures and
returns the first sub-path whose `name` matches. The four consumers in the
codebase:

### 1. Template function `module`

Returns a module helper used to render qualified identifiers and schedule
imports. Accepts absolute paths, relative paths (`../module`), and structure
references:

```gotemplate
{{ $entity := module "structure:domain.entity" -}}
{{ $entity.Ident "Order" }}
```

The helper resolves to the module containing the structure's sub-path, so
generated code emits the correct import and qualified name regardless of the
consuming module.

or, when you just need to declare an import statement without using the module helper, you can use the

```gotemplate
{{ module_import "structure:domain.entity" -}}

```

### 2. Template function `path_join`

Joins multiple path segments and runs each segment through the structure
resolver, so `structure:<name>` parts expand to their resolved filesystem
path before joining:

```gotemplate
{{ path_join "structure:domain.entity" "subdir" }}
```

### 3. `structure:` field on annotations and macros

In `plumber.yml`, options and macros can declare a target structure.
When the annotation runs, generated output is routed into the resolved
directory — authors do not hard-code paths:

```yaml
plumber.shape:
  options:
    - plumber.option:
        name: plumber:domain
        structure: structure:domain.entity
        # ...
```

### 4. `plumber shape structure` and `Required: yes`

Sub-paths flagged `required: true` are scaffolded by the `shape structure`
subcommand using their declared `template`. This is why the describe table
surfaces `Required` — `yes` means the directory will be created (and an
initial file rendered) on the next `plumber shape structure` run; `no` means
the path exists only as a target for `module "structure:..."` and
`path_join`.

```bash
plumber shape -c plumber.shape.yaml structure
```

## Editing structures

Structures live under `plumber.shape.structures` in `plumber.yml`
(or any file pulled in via `includes`). A minimal entry:

```yaml
plumber.shape:
  structures:
    - plumber.structure:
        name: hexagonal.v1
        title: Hexagonal architecture structure
        documentation: |
          Long-form prose explaining when this structure applies.
        path: internal/{{ .Module.NormalizedName }}
        paths:
          - plumber.path:
              name: domain.entity
              title: Domain entities
              path: domain/entity
              template: hexagonal.v1/domain.entity    # templates included when rendering a scaffolded sub-path
              templates: [hexagonal.v1/domain.entity] # templates included when rendering a scaffolded sub-path
              required: true                         # scaffolded by `shape structure`
              description: |
                File description doc emitted when the sub-path is scaffolded.
              package_description: |
                Package doc comment emitted when the sub-path is scaffolded.
```

Notes:

- `path` on the structure can use `{{ .Module.* }}` / `{{ .Repo.* }}` —
  expansion happens at describe / generation time, so the same structure
  definition can be reused across modules.
- `name` on a sub-path is the identifier exposed via `structure:<name>`;
  keep it stable, since it is referenced from templates and annotations.
- `title` on the sub-path is what `shape describe structure` uses as its
  heading; keep it short.
- `documentation` is long-form prose surfaced as the body paragraph.
- See the `or-plumber-shape` skill for the surrounding `plumber.shape.yaml`
  shape (sources, templates, macros, mixins).

## Key rules for agents

- **Run `plumber shape -c <config> describe structure` before authoring** a
  macro, mixin, or template that targets a structure path. Do **not** infer
  paths from filesystem layout — they vary per module.
- **Reference structures by `structure:<name>`**, never by hard-coded relative
  paths.
- **`Required: yes` paths** are scaffolded by `plumber shape structure`;
  non-required paths exist only as resolution targets for `module` and
  `path_join`.
- **`-c` lives on `shape`**, not on the subcommand: write
  `plumber shape -c plumber.shape.yaml describe structure`, not
  `plumber shape describe structure -c plumber.shape.yaml`.
- **Use `-f json|yaml`** when consuming the inventory from another tool;
  the markdown form is for reading.
