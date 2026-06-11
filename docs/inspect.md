# Plumber Inspect

The `inspect` command **scans one or more Go packages** and emits structured information
about the packages, types, and functions it finds.  The output is intended for consumption
by external tools, scripts, or AI agents that need a structured view of the codebase.

## Quick start

```shell
go run github.com/getoutreach/plumber/cmd/plumber@version inspect [--format json|yaml] ./...
```

Use standard Go package pattern syntax (`./...`, `./internal/pkg`, etc.).

---

## Output formats

| Flag value | Description |
|---|---|
| `json` *(default)* | Compact JSON array of package objects |
| `yaml` | YAML array of package objects |

Pass the format with `--format` / `-f`:

```shell
plumber inspect --format yaml ./...
```

---

## Output schema

The top-level output is a JSON/YAML **array of Package objects**. Each package contains:

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
│   ├── struct      {fields[]}         — present when kind == "struct"
│   └── interface   {methods[]}        — present when kind == "interface"
├── functions[]
│   ├── name        string
│   ├── position    {filename, line, column}
│   ├── doc         string
│   └── annotations [{name, args, namedArgs}]
└── comments[]     — package-level comment groups with annotations
```

---

## Example usage

### List all interface types

```shell
plumber inspect --format json ./... \
  | jq '.[]?.types[]? | select(.interface) | {name: .name, type: .spec.fqn}'
```

```json
{"name": "Closer",         "type": "\"github.com/getoutreach/plumber/example/contract\".Closer"}
{"name": "MutatorService", "type": "\"github.com/getoutreach/plumber/example/contract\".MutatorService"}
{"name": "OpenCloser",     "type": "\"github.com/getoutreach/plumber/example/contract\".OpenCloser"}
{"name": "Repository",     "type": "\"github.com/getoutreach/plumber/example/contract\".Repository"}
```

### List all struct names across packages

```shell
plumber inspect --format json ./... \
  | jq '[.[]?.types[]? | select(.struct) | .name]'
```

### Show plumber annotations on types

```shell
plumber inspect --format json ./... \
  | jq '.[]?.types[]? | select(.annotations | length > 0) | {name, annotations}'
```

---

## Configuration file (`plumber.shape.yaml`)

The `inspect` command shares the same YAML configuration file as `shape`.  Pass it with
`--config` / `-c`:

```shell
plumber inspect --config plumber.shape.yaml --format json ./...
```

The relevant section is `plumber.inspect`:

```yaml
plumber.inspect:
  format: json          # "json" or "yaml" — overridden by --format flag
  annotations:          # optional annotation filter list (reserved for future use)
    - list: []
```

When both the config file and the `--format` flag are provided, the **flag takes
precedence**.

---

## Command flags

| Flag | Alias | Default | Description |
|---|---|---|---|
| `--config` | `-c` | — | Path to `plumber.shape.yaml` (optional) |
| `--format` | `-f` | `json` | Output format: `json` or `yaml` |

---

## Integration with `shape`

The inspect pipeline is also used **internally** by the `shape` command before any code
generation: `shape` calls the same `inspect.ScanFiles` and `inspect.Inspect` functions to
build the type model that drives all transformations.  Running `inspect` manually therefore
gives a transparent view of exactly what data `shape` operates on.
