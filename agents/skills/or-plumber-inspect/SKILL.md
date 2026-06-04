---
name: or-plumber-inspect
description: "Provides guidance on plumber's inspect command for emitting structured JSON/YAML metadata about Go packages, types, functions, and annotations."
---

# Inspect

The `inspect` command scans Go packages and emits structured metadata about packages,
types, functions, and annotations. The output is intended for external tools, scripts, or
AI agents that need a structured view of the codebase.

## When to use

- You need structured type/function information from Go packages for tooling or analysis.
- You want to see exactly what the `shape` command sees before code generation.
- You need to query annotations, interfaces, or struct fields programmatically.

## CLI

```bash
go run cmd/plumber/plumber.go inspect [--config plumber.shape.yaml] [--format json|yaml] ./...
```

| Flag | Alias | Default | Description |
|---|---|---|---|
| `--config` | `-c` | — | Path to `plumber.shape.yaml` (optional) |
| `--format` | `-f` | `json` | Output format: `json` or `yaml` |

Standard Go package patterns: `./...`, `./internal/pkg`, etc.

## Output schema

Top-level output is an array of Package objects:

```
Package
├── name        string            — package name
├── path        string            — fully-qualified import path
├── types[]
│   ├── name        string
│   ├── position    {filename, line, column}
│   ├── doc         string
│   ├── annotations [{name, args, namedArgs}]
│   ├── spec        {kind, fqn, ...}
│   ├── struct      {fields[]}         — when kind == "struct"
│   └── interface   {methods[]}        — when kind == "interface"
├── functions[]
│   ├── name        string
│   ├── position    {filename, line, column}
│   ├── doc         string
│   └── annotations [{name, args, namedArgs}]
└── comments[]     — package-level comment groups with annotations
```

## Example queries

### List all interface types

```bash
plumber inspect --format json ./... \
  | jq '.[]?.types[]? | select(.interface) | {name: .name, type: .spec.fqn}'
```

### List all struct names

```bash
plumber inspect --format json ./... \
  | jq '[.[]?.types[]? | select(.struct) | .name]'
```

### Show annotated types

```bash
plumber inspect --format json ./... \
  | jq '.[]?.types[]? | select(.annotations | length > 0) | {name, annotations}'
```

## Configuration

Shares the same `plumber.shape.yaml` config file as `shape`. The relevant section:

```yaml
plumber.inspect:
  format: json
```

When both the config file and `--format` flag are provided, the flag takes precedence.

## Integration with shape

The inspect pipeline is used internally by `shape` before code generation. Running
`inspect` manually gives a transparent view of exactly what data `shape` operates on.

## Key rules for agents

- **Use `jq` to filter output** — the JSON output can be large for `./...` scans.
- **Inspect shows what shape sees** — use it to debug annotation issues before running shape.
- **Annotations array is empty** for types without `plumber:*` comments — filter with `select(.annotations | length > 0)`.
