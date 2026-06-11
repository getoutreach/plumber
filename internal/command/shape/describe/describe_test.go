// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains unit tests for the describe package's Build function and formatters.

package describe

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/structure"
)

func TestBuildEmpty(t *testing.T) {
	cfg := &shape.Config{}
	desc := Build(cfg, &structure.NoopResolver{})

	if len(desc.Macros) != 0 {
		t.Errorf("expected 0 macros, got %d", len(desc.Macros))
	}
	if len(desc.Options) != 0 {
		t.Errorf("expected 0 options, got %d", len(desc.Options))
	}
	if len(desc.Handlers) != 0 {
		t.Errorf("expected 0 handlers, got %d", len(desc.Handlers))
	}
}

func TestBuildMacros(t *testing.T) {
	cfg := &shape.Config{
		Macros: []config.MacroConfig{
			{
				PlumberMacro: &config.PlumberMacroConfig{
					AnnotationSchemaConfig: config.AnnotationSchemaConfig{
						Name: "test-macro",
						Doc: config.DocumentationConfig{
							Description: "A macro for testing.",
						},
					},
					Options: []config.OptionReferenceConfig{
						{Name: "plumber:template"},
						{Name: "plumber:context"},
					},
				},
			},
			{PlumberMacro: nil}, // nil entry should be skipped
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})

	if len(desc.Macros) != 1 {
		t.Fatalf("expected 1 macro, got %d", len(desc.Macros))
	}
	m := desc.Macros[0]
	if m.Name != "test-macro" {
		t.Errorf("expected name 'test-macro', got %q", m.Name)
	}
	if m.Doc.Description != "A macro for testing." {
		t.Errorf("expected description 'A macro for testing.', got %q", m.Doc.Description)
	}
	if len(m.Options) != 2 {
		t.Errorf("expected 2 options, got %d", len(m.Options))
	}
}

func TestBuildOptions(t *testing.T) {
	cfg := &shape.Config{
		Options: []config.AnnotationSchemaConfig{
			{
				Name: "plumber:template",
				Doc: config.DocumentationConfig{
					Description: "Template annotation.",
				},
			},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})

	if len(desc.Options) != 1 {
		t.Fatalf("expected 1 option, got %d", len(desc.Options))
	}
	o := desc.Options[0]
	if o.Name != "plumber:template" {
		t.Errorf("expected name 'plumber:template', got %q", o.Name)
	}
	if o.Doc.Description != "Template annotation." {
		t.Errorf("unexpected description: %q", o.Doc.Description)
	}
}

func TestBuildHandlers(t *testing.T) {
	cfg := &shape.Config{
		Handlers: []config.HandlerConfig{
			{
				PlumberHandler: &config.PlumberHandlerConfig{
					Doc: config.DocumentationConfig{
						Description: "Runs gofmt on changed files.",
					},
					Name:    "format",
					Command: "gofmt -w {{ .Source.NamedArgs.path }}",
				},
			},
			{PlumberHandler: nil}, // nil entry should be skipped
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})

	if len(desc.Handlers) != 1 {
		t.Fatalf("expected 1 handler, got %d", len(desc.Handlers))
	}
	h := desc.Handlers[0]
	if h.Name != "format" {
		t.Errorf("expected name 'format', got %q", h.Name)
	}
	if h.Command != "gofmt -w {{ .Source.NamedArgs.path }}" {
		t.Errorf("unexpected command: %q", h.Command)
	}
	if h.Doc.Description != "Runs gofmt on changed files." {
		t.Errorf("unexpected description: %q", h.Doc.Description)
	}
}

func TestIntrospectPositionalHomogeneous(t *testing.T) {
	var node yaml.Node
	schemaYAML := `type: array
minItems: 1
items:
  type: string
  description: Template name to load.`
	if err := yaml.Unmarshal([]byte(schemaYAML), &node); err != nil {
		t.Fatal(err)
	}

	cfg := &shape.Config{
		Options: []config.AnnotationSchemaConfig{
			{
				Name: "plumber:template",
				Schema: &config.AnnotationArgumentSchemaConfig{
					Positional: *node.Content[0],
				},
			},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})
	schema := desc.Options[0].Schema
	if schema == nil {
		t.Fatal("expected schema to be non-nil")
	}
	if schema.Positional == nil {
		t.Fatal("expected positional schema to be non-nil")
	}
	if len(schema.Positional.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(schema.Positional.Items))
	}
	item := schema.Positional.Items[0]
	if item.Position != "*" {
		t.Errorf("expected position '*', got %q", item.Position)
	}
	if item.Type != "string" {
		t.Errorf("expected type 'string', got %q", item.Type)
	}
	if item.Description != "Template name to load." {
		t.Errorf("unexpected description: %q", item.Description)
	}
}

func TestIntrospectPositionalTuple(t *testing.T) {
	var node yaml.Node
	schemaYAML := `type: array
minItems: 1
maxItems: 2
items:
  - type: string
    description: Filter function name.
    enum:
      - annotation.has
  - type: string
    description: Filter argument.`
	if err := yaml.Unmarshal([]byte(schemaYAML), &node); err != nil {
		t.Fatal(err)
	}

	cfg := &shape.Config{
		Options: []config.AnnotationSchemaConfig{
			{
				Name: "plumber:filter",
				Schema: &config.AnnotationArgumentSchemaConfig{
					Positional: *node.Content[0],
				},
			},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})
	schema := desc.Options[0].Schema
	if schema == nil || schema.Positional == nil {
		t.Fatal("expected positional schema")
	}
	if len(schema.Positional.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(schema.Positional.Items))
	}

	item0 := schema.Positional.Items[0]
	if item0.Position != "0" {
		t.Errorf("expected position '0', got %q", item0.Position)
	}
	if !strings.Contains(item0.Details, "enum: annotation.has") {
		t.Errorf("expected enum in details, got %q", item0.Details)
	}

	item1 := schema.Positional.Items[1]
	if item1.Position != "1" {
		t.Errorf("expected position '1', got %q", item1.Position)
	}
}

func TestIntrospectNamedWithRequired(t *testing.T) {
	var node yaml.Node
	schemaYAML := `type: object
required:
  - scope
properties:
  scope:
    type: string
    description: Package path to search within.
  receiver:
    type: string
    description: Variable name for field access.`
	if err := yaml.Unmarshal([]byte(schemaYAML), &node); err != nil {
		t.Fatal(err)
	}

	cfg := &shape.Config{
		Options: []config.AnnotationSchemaConfig{
			{
				Name: "plumber:query",
				Schema: &config.AnnotationArgumentSchemaConfig{
					Named: *node.Content[0],
				},
			},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})
	schema := desc.Options[0].Schema
	if schema == nil || schema.Named == nil {
		t.Fatal("expected named schema")
	}

	// Find scope property
	var scopeFound, receiverFound bool
	for _, p := range schema.Named.Properties {
		switch p.Name {
		case "scope":
			scopeFound = true
			if !p.Required {
				t.Error("expected scope to be required")
			}
		case "receiver":
			receiverFound = true
			if p.Required {
				t.Error("expected receiver to not be required")
			}
		}
	}
	if !scopeFound {
		t.Error("scope property not found")
	}
	if !receiverFound {
		t.Error("receiver property not found")
	}
}

func TestIntrospectNamedAdditionalProperties(t *testing.T) {
	var node yaml.Node
	schemaYAML := `type: object
additionalProperties:
  type: string
  description: Named arguments passed to handler.`
	if err := yaml.Unmarshal([]byte(schemaYAML), &node); err != nil {
		t.Fatal(err)
	}

	cfg := &shape.Config{
		Options: []config.AnnotationSchemaConfig{
			{
				Name: "plumber:notify",
				Schema: &config.AnnotationArgumentSchemaConfig{
					Named: *node.Content[0],
				},
			},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})
	schema := desc.Options[0].Schema
	if schema == nil || schema.Named == nil {
		t.Fatal("expected named schema")
	}
	if len(schema.Named.Properties) != 1 {
		t.Fatalf("expected 1 property (wildcard), got %d", len(schema.Named.Properties))
	}
	if schema.Named.Properties[0].Name != "*" {
		t.Errorf("expected wildcard property name '*', got %q", schema.Named.Properties[0].Name)
	}
}

func TestJSONFormatter(t *testing.T) {
	desc := Description{
		Macros:   []MacroDescription{{Name: "m1", Doc: DocDescription{Description: "Macro one"}}},
		Options:  []OptionDescription{{Name: "o1", Doc: DocDescription{Description: "Option one"}}},
		Handlers: []HandlerDescription{{Name: "h1", Command: "cmd", Doc: DocDescription{Description: "Handler one"}}},
	}

	f, err := Format("json")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.Format(desc)
	if err != nil {
		t.Fatal(err)
	}

	// Verify it's valid JSON
	var parsed Description
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if parsed.Macros[0].Name != "m1" {
		t.Errorf("expected macro name 'm1', got %q", parsed.Macros[0].Name)
	}
}

func TestYAMLFormatter(t *testing.T) {
	desc := Description{
		Macros:   []MacroDescription{{Name: "m1", Doc: DocDescription{Description: "Macro one"}}},
		Options:  []OptionDescription{},
		Handlers: []HandlerDescription{},
	}

	f, err := Format("yaml")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.Format(desc)
	if err != nil {
		t.Fatal(err)
	}

	// Verify it's valid YAML
	var parsed Description
	if err := yaml.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("output is not valid YAML: %v", err)
	}
	if parsed.Macros[0].Name != "m1" {
		t.Errorf("expected macro name 'm1', got %q", parsed.Macros[0].Name)
	}
}

func TestMDFormatter(t *testing.T) {
	desc := Description{
		Macros: []MacroDescription{
			{
				Name:    "derive",
				Doc:     DocDescription{Description: "Derives stuff."},
				Options: []string{"plumber:context"},
			},
		},
		Options: []OptionDescription{
			{Name: "plumber:context", Doc: DocDescription{Description: "Context annotation."}},
		},
		Handlers: []HandlerDescription{
			{Name: "fmt", Command: "gofmt -w .", Doc: DocDescription{Description: "Format code."}},
		},
	}

	f, err := Format("md")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.Format(desc)
	if err != nil {
		t.Fatal(err)
	}

	s := string(out)
	if !strings.Contains(s, "# Shape Configuration") {
		t.Error("missing top-level heading")
	}
	if !strings.Contains(s, "## Macros") {
		t.Error("missing Macros section")
	}
	if !strings.Contains(s, "### derive") {
		t.Error("missing derive macro heading")
	}
	if !strings.Contains(s, "## Options") {
		t.Error("missing Options section")
	}
	if !strings.Contains(s, "## Handlers") {
		t.Error("missing Handlers section")
	}
	if !strings.Contains(s, "`gofmt -w .`") {
		t.Error("missing handler command")
	}
}

func TestMDFormatterWithSchema(t *testing.T) {
	var positionalNode yaml.Node
	schemaYAML := `type: array
minItems: 1
items:
  - type: string
    description: Filter function.
    enum:
      - annotation.has
  - type: string
    description: Filter arg.`
	if err := yaml.Unmarshal([]byte(schemaYAML), &positionalNode); err != nil {
		t.Fatal(err)
	}

	var namedNode yaml.Node
	namedYAML := `type: object
required:
  - scope
properties:
  scope:
    type: string
    description: Target scope.`
	if err := yaml.Unmarshal([]byte(namedYAML), &namedNode); err != nil {
		t.Fatal(err)
	}

	cfg := &shape.Config{
		Options: []config.AnnotationSchemaConfig{
			{
				Name: "plumber:filter",
				Schema: &config.AnnotationArgumentSchemaConfig{
					Positional: *positionalNode.Content[0],
					Named:      *namedNode.Content[0],
				},
			},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})
	f, err := Format("md")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.Format(desc)
	if err != nil {
		t.Fatal(err)
	}

	s := string(out)
	if !strings.Contains(s, "**Positional arguments:**") {
		t.Error("missing positional arguments header")
	}
	if !strings.Contains(s, "| # | Type | Description | Required | Details |") {
		t.Error("missing positional table header")
	}
	if !strings.Contains(s, "annotation.has") {
		t.Error("missing enum value in table")
	}
	if !strings.Contains(s, "**Named arguments:**") {
		t.Error("missing named arguments header")
	}
	if !strings.Contains(s, "| scope | string |") {
		t.Error("missing scope property in named table")
	}
	if !strings.Contains(s, "**Schema definition:**") {
		t.Error("missing schema definition section")
	}
}

func TestGetUnknownFormat(t *testing.T) {
	_, err := Format("xml")
	if err == nil {
		t.Fatal("expected error for unknown format")
	}
	if !strings.Contains(err.Error(), "unknown format") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestFormats(t *testing.T) {
	names := Formats()
	if len(names) < 3 {
		t.Errorf("expected at least 3 formats, got %d", len(names))
	}
}

func TestBuildMacroHandlersFromOptions(t *testing.T) {
	cfg := &shape.Config{
		Macros: []config.MacroConfig{
			{
				PlumberMacro: &config.PlumberMacroConfig{
					AnnotationSchemaConfig: config.AnnotationSchemaConfig{
						Name: "my-macro",
					},
					Options: []config.OptionReferenceConfig{
						{Name: "plumber:template"},
						{Name: "plumber:context"},
						{Name: "plumber:output"},
					},
				},
			},
		},
		Options: []config.AnnotationSchemaConfig{
			{Name: "plumber:template", Handler: "handler-a"},
			{Name: "plumber:context"},
			{Name: "plumber:output", Handler: "handler-b"},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})

	if len(desc.Macros) != 1 {
		t.Fatalf("expected 1 macro, got %d", len(desc.Macros))
	}
}

func TestMdFormatHandlersRow(t *testing.T) {
	desc := Description{
		Macros: []MacroDescription{
			{
				Name: "macro-with-handlers",
				Metadata: MetadataDescription{
					Handler: "h1",
				},
			},
		},
	}

	f, err := Format("md")
	if err != nil {
		t.Fatalf("unexpected error getting formatter: %v", err)
	}
	out, err := f.Format(desc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, "| Handler |") {
		t.Error("expected Handler row in metadata table")
	}
	if !strings.Contains(s, "`h1`") {
		t.Errorf("expected handler value in output, got:\n%s", s)
	}
}

func TestBuildHandlersWithArgsAndVariants(t *testing.T) {
	cfg := &shape.Config{
		Handlers: []config.HandlerConfig{
			{
				PlumberHandler: &config.PlumberHandlerConfig{
					Name:    "build",
					Command: "go build ./...",
					Args:    []string{"-v", "-race"},
					Variants: []config.HandlerVariantConfig{
						{Name: "fast", Command: "go build -ldflags=-s ./...", Args: []string{"-v"}},
						{Name: "debug", Command: "go build -gcflags=all=-N ./..."},
					},
				},
			},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})

	if len(desc.Handlers) != 1 {
		t.Fatalf("expected 1 handler, got %d", len(desc.Handlers))
	}
	h := desc.Handlers[0]
	if len(h.Args) != 2 || h.Args[0] != "-v" || h.Args[1] != "-race" {
		t.Errorf("expected args [-v -race], got %v", h.Args)
	}
}

func TestMdFormatHandlerArgsAndVariants(t *testing.T) {
	desc := Description{
		Handlers: []HandlerDescription{
			{
				Name:    "build",
				Command: "go build",
				Args:    []string{"-v", "-race"},
			},
		},
	}

	f, err := Format("md")
	if err != nil {
		t.Fatalf("unexpected error getting formatter: %v", err)
	}
	out, err := f.Format(desc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, "**Args:**") {
		t.Error("expected Args section in handler output")
	}
	if !strings.Contains(s, "`-v`") {
		t.Error("expected -v arg in handler output")
	}
}

func TestBuildOptionsWithUsage(t *testing.T) {
	cfg := &shape.Config{
		Options: []config.AnnotationSchemaConfig{
			{
				Name: "plumber:template",
				Doc: config.DocumentationConfig{
					Description: "Template annotation.",
					Usage:       "plumber:template <name>",
				},
			},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})

	if len(desc.Options) != 1 {
		t.Fatalf("expected 1 option, got %d", len(desc.Options))
	}
	o := desc.Options[0]
	if o.Doc.Usage != "plumber:template <name>" {
		t.Errorf("expected usage 'plumber:template <name>', got %q", o.Doc.Usage)
	}
}

func TestBuildOptionsWithStructure(t *testing.T) {
	cfg := &shape.Config{
		Options: []config.AnnotationSchemaConfig{
			{
				Name:      "plumber:generate",
				Structure: "structure:models",
			},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})

	if len(desc.Options) != 1 {
		t.Fatalf("expected 1 option, got %d", len(desc.Options))
	}
	o := desc.Options[0]
	if o.Structure == nil {
		t.Fatal("expected structure to be non-nil")
	}
	if o.Structure.Name != "models" {
		t.Errorf("expected structure name 'models', got %q", o.Structure.Name)
	}
	// NoopResolver returns the raw path as-is
	if o.Structure.Path != "structure:models" {
		t.Errorf("expected structure path 'structure:models', got %q", o.Structure.Path)
	}
}

func TestBuildOptionsWithoutStructure(t *testing.T) {
	cfg := &shape.Config{
		Options: []config.AnnotationSchemaConfig{
			{
				Name: "plumber:template",
			},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})

	if desc.Options[0].Structure != nil {
		t.Error("expected structure to be nil when not set")
	}
}

func TestBuildMacrosWithStructure(t *testing.T) {
	cfg := &shape.Config{
		Macros: []config.MacroConfig{
			{
				PlumberMacro: &config.PlumberMacroConfig{
					AnnotationSchemaConfig: config.AnnotationSchemaConfig{
						Name:      "my-macro",
						Structure: "structure:services",
					},
				},
			},
		},
	}

	desc := Build(cfg, &structure.NoopResolver{})

	if len(desc.Macros) != 1 {
		t.Fatalf("expected 1 macro, got %d", len(desc.Macros))
	}
	m := desc.Macros[0]
	if m.Structure == nil {
		t.Fatal("expected structure to be non-nil")
	}
	if m.Structure.Name != "services" {
		t.Errorf("expected structure name 'services', got %q", m.Structure.Name)
	}
}

func TestMdFormatUsage(t *testing.T) {
	desc := Description{
		Options: []OptionDescription{
			{
				Name: "plumber:template",
				Doc:  DocDescription{Description: "Template annotation.", Usage: "plumber:template <name>"},
			},
		},
	}

	f, err := Format("md")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.Format(desc)
	if err != nil {
		t.Fatal(err)
	}

	s := string(out)
	if !strings.Contains(s, "**Usage:** `plumber:template <name>`") {
		t.Errorf("expected usage in markdown output, got:\n%s", s)
	}
}

func TestMdFormatStructure(t *testing.T) {
	desc := Description{
		Options: []OptionDescription{
			{
				Name:      "plumber:generate",
				Structure: &StructureDescription{Name: "models", Path: "/resolved/path/models"},
			},
		},
	}

	f, err := Format("md")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.Format(desc)
	if err != nil {
		t.Fatal(err)
	}

	s := string(out)
	if !strings.Contains(s, "**Structure:** models") {
		t.Errorf("expected structure name in markdown output, got:\n%s", s)
	}
	if !strings.Contains(s, "`/resolved/path/models`") {
		t.Errorf("expected structure path in markdown output, got:\n%s", s)
	}
}
