// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains unit tests for JSON Schema-based annotation validation.

package validate

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/query/model"
	"gopkg.in/yaml.v3"
	"gotest.tools/v3/assert"
)

// yamlNode parses a YAML string into a yaml.Node suitable for schema fields.
func yamlNode(t *testing.T, s string) yaml.Node {
	t.Helper()
	var node yaml.Node
	err := yaml.Unmarshal([]byte(s), &node)
	assert.NilError(t, err)
	// yaml.Unmarshal wraps in a document node; return its first content node.
	if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		return *node.Content[0]
	}
	return node
}

func TestCompileSchema_NilSchema(t *testing.T) {
	cs, err := CompileSchema("test", nil)
	assert.NilError(t, err)
	assert.Assert(t, cs == nil)
}

func TestCompileSchema_PositionalOnly(t *testing.T) {
	schema := &config.AnnotationArgumentSchemaConfig{
		Positional: yamlNode(t, `
type: array
minItems: 1
items:
  type: string
`),
	}
	cs, err := CompileSchema("plumber:template", schema)
	assert.NilError(t, err)
	assert.Assert(t, cs != nil)
	assert.Assert(t, cs.Positional != nil)
	assert.Assert(t, cs.Named == nil)
}

func TestCompileSchema_NamedOnly(t *testing.T) {
	schema := &config.AnnotationArgumentSchemaConfig{
		Named: yamlNode(t, `
type: object
required: [scope]
properties:
  scope:
    type: string
`),
	}
	cs, err := CompileSchema("plumber:query", schema)
	assert.NilError(t, err)
	assert.Assert(t, cs != nil)
	assert.Assert(t, cs.Positional == nil)
	assert.Assert(t, cs.Named != nil)
}

func TestAnnotation_PositionalValid(t *testing.T) {
	schema := &config.AnnotationArgumentSchemaConfig{
		Positional: yamlNode(t, `
type: array
minItems: 1
items:
  type: string
`),
	}
	cs, err := CompileSchema("plumber:template", schema)
	assert.NilError(t, err)

	ann := model.NewAnnotation("plumber:template", []string{"my-template"})
	err = Annotation(ann, cs)
	assert.NilError(t, err)
}

func TestAnnotation_PositionalTooFewArgs(t *testing.T) {
	schema := &config.AnnotationArgumentSchemaConfig{
		Positional: yamlNode(t, `
type: array
minItems: 1
items:
  type: string
`),
	}
	cs, err := CompileSchema("plumber:template", schema)
	assert.NilError(t, err)

	ann := model.NewAnnotation("plumber:template", nil)
	err = Annotation(ann, cs)
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "positional args")
}

func TestAnnotation_PositionalTooManyArgs(t *testing.T) {
	schema := &config.AnnotationArgumentSchemaConfig{
		Positional: yamlNode(t, `
type: array
maxItems: 1
items:
  type: string
`),
	}
	cs, err := CompileSchema("plumber:mode", schema)
	assert.NilError(t, err)

	ann := model.NewAnnotation("plumber:mode", []string{"generated", "extra"})
	err = Annotation(ann, cs)
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "positional args")
}

func TestAnnotation_PositionalEnumValid(t *testing.T) {
	schema := &config.AnnotationArgumentSchemaConfig{
		Positional: yamlNode(t, `
type: array
maxItems: 1
items:
  type: string
  enum: [generated, inplace]
`),
	}
	cs, err := CompileSchema("plumber:mode", schema)
	assert.NilError(t, err)

	ann := model.NewAnnotation("plumber:mode", []string{"inplace"})
	err = Annotation(ann, cs)
	assert.NilError(t, err)
}

func TestAnnotation_PositionalEnumInvalid(t *testing.T) {
	schema := &config.AnnotationArgumentSchemaConfig{
		Positional: yamlNode(t, `
type: array
maxItems: 1
items:
  type: string
  enum: [generated, inplace]
`),
	}
	cs, err := CompileSchema("plumber:mode", schema)
	assert.NilError(t, err)

	ann := model.NewAnnotation("plumber:mode", []string{"unknown"})
	err = Annotation(ann, cs)
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "positional args")
}

func TestAnnotation_NamedRequiredPresent(t *testing.T) {
	schema := &config.AnnotationArgumentSchemaConfig{
		Positional: yamlNode(t, `
type: array
minItems: 1
maxItems: 1
items:
  type: string
`),
		Named: yamlNode(t, `
type: object
required: [scope]
properties:
  scope:
    type: string
  receiver:
    type: string
`),
	}
	cs, err := CompileSchema("plumber:query", schema)
	assert.NilError(t, err)

	ann := model.NewAnnotation("plumber:query", []string{"^Init.*"},
		model.WithNamedArgs(map[string]string{"scope": "."}))
	err = Annotation(ann, cs)
	assert.NilError(t, err)
}

func TestAnnotation_NamedRequiredMissing(t *testing.T) {
	schema := &config.AnnotationArgumentSchemaConfig{
		Positional: yamlNode(t, `
type: array
minItems: 1
maxItems: 1
items:
  type: string
`),
		Named: yamlNode(t, `
type: object
required: [scope]
properties:
  scope:
    type: string
`),
	}
	cs, err := CompileSchema("plumber:query", schema)
	assert.NilError(t, err)

	ann := model.NewAnnotation("plumber:query", []string{"^Init.*"})
	err = Annotation(ann, cs)
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "named args")
}

func TestAnnotation_NilSchema(t *testing.T) {
	ann := model.NewAnnotation("plumber:template", []string{"foo"})
	err := Annotation(ann, nil)
	assert.NilError(t, err)
}

func TestCompileSchemas_BatchCompile(t *testing.T) {
	schemas := []config.AnnotationSchemaConfig{
		{
			Name: "plumber:template",
			Schema: &config.AnnotationArgumentSchemaConfig{
				Positional: yamlNode(t, `
type: array
minItems: 1
items:
  type: string
`),
			},
		},
		{
			Name: "plumber:mode",
			Schema: &config.AnnotationArgumentSchemaConfig{
				Positional: yamlNode(t, `
type: array
maxItems: 1
items:
  type: string
  enum: [generated, inplace]
`),
			},
		},
		{
			Name:   "plumber:no-schema",
			Schema: nil, // no schema — should be skipped
		},
	}

	compiled, err := CompileSchemas(schemas)
	assert.NilError(t, err)
	assert.Equal(t, len(compiled), 2)
	assert.Assert(t, compiled["plumber:template"] != nil)
	assert.Assert(t, compiled["plumber:mode"] != nil)
}

func TestAnnotations_MixedValid(t *testing.T) {
	schemas := []config.AnnotationSchemaConfig{
		{
			Name: "plumber:mode",
			Schema: &config.AnnotationArgumentSchemaConfig{
				Positional: yamlNode(t, `
type: array
maxItems: 1
items:
  type: string
  enum: [generated, inplace]
`),
			},
		},
	}
	compiled, err := CompileSchemas(schemas)
	assert.NilError(t, err)

	annotations := model.Annotations{
		model.NewAnnotation("plumber:template", []string{"my-tpl"}), // no schema, skipped
		model.NewAnnotation("plumber:mode", []string{"generated"}),  // valid
	}

	err = Annotations(annotations, compiled)
	assert.NilError(t, err)
}

func TestAnnotations_FailsOnInvalid(t *testing.T) {
	schemas := []config.AnnotationSchemaConfig{
		{
			Name: "plumber:mode",
			Schema: &config.AnnotationArgumentSchemaConfig{
				Positional: yamlNode(t, `
type: array
maxItems: 1
items:
  type: string
  enum: [generated, inplace]
`),
			},
		},
	}
	compiled, err := CompileSchemas(schemas)
	assert.NilError(t, err)

	annotations := model.Annotations{
		model.NewAnnotation("plumber:mode", []string{"badvalue"}),
	}

	err = Annotations(annotations, compiled)
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "plumber:mode")
}

func TestAnnotation_DefaultsYAMLFromDefaults(t *testing.T) {
	// Test with a schema matching the actual defaults.yaml structure for plumber:query
	schema := &config.AnnotationArgumentSchemaConfig{
		Positional: yamlNode(t, `
type: array
minItems: 1
maxItems: 1
items:
  type: string
  description: Regex pattern to match entity names within the scope.
`),
		Named: yamlNode(t, `
type: object
required:
  - scope
properties:
  scope:
    type: string
    description: Package path or type FQN to search within.
  receiver:
    type: string
    description: Variable name to qualify field/method access.
`),
	}

	cs, err := CompileSchema("plumber:query", schema)
	assert.NilError(t, err)

	// Valid: has pattern and scope
	ann := model.NewAnnotation("plumber:query", []string{"^Get.*"},
		model.WithNamedArgs(map[string]string{"scope": ".", "receiver": "r"}))
	assert.NilError(t, Annotation(ann, cs))

	// Invalid: missing required scope
	annNoScope := model.NewAnnotation("plumber:query", []string{"^Get.*"})
	err = Annotation(annNoScope, cs)
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "named args")

	// Invalid: no positional args
	annNoArgs := model.NewAnnotation("plumber:query", nil,
		model.WithNamedArgs(map[string]string{"scope": "."}))
	err = Annotation(annNoArgs, cs)
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "positional args")
}
