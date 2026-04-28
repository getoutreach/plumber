// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains unit tests for macro expansion in expandAnnotations
// and the deferred per-annotation template expansion performed by TransformerAnnotations.

package expand

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"gotest.tools/v3/assert"
)

// fixturePackage returns a minimal *model.Package suitable for driving template
// expansion in macro tests. The Name/Path values are echoed back through
// .Package.Name / .Package.Path inside the macro template context.
func fixturePackage() *model.Package {
	return &model.Package{
		Name: "fixture",
		Path: "example.com/fixture",
	}
}

// fixtureNode wraps the fixturePackage in a *model.Type so it satisfies the
// model.Node interface required by TransformerAnnotations. Tests that need to
// drive .Type-aware templates can extend the returned node before passing it in.
func fixtureNode() model.Node {
	return &model.Type{
		TypeNode: &model.TypeNode{
			Package: fixturePackage(),
			Position: model.Position{
				Filename: "fixture.go",
			},
		},
		Name: "FixtureType",
	}
}

// expandAndTransform runs the two-stage pipeline: macro expansion (which only
// substitutes the macro annotation with its child annotation list and records
// ImpliedBy on each child) followed by the deferred per-annotation template
// expansion performed by TransformerAnnotations. Helper used by tests that
// previously asserted on the combined behaviour.
func expandAndTransform(
	t *testing.T,
	node model.Node,
	input model.Annotations,
	macroMap map[string]*config.PlumberMacroConfig,
) (model.Annotations, error) {
	t.Helper()
	var pkg *model.Package
	if node != nil {
		pkg = node.GetPackage()
	}
	expanded, err := expandAnnotations(pkg, input, macroMap)
	if err != nil {
		return nil, err
	}
	return TransformerAnnotations(node, expanded, render.Scope{})
}

func TestExpandAnnotations_NoMacro(t *testing.T) {
	macroMap := map[string]*config.PlumberMacroConfig{}
	input := model.Annotations{
		model.NewAnnotation("plumber:derive", []string{"Foo"}),
	}

	result, err := expandAnnotations(fixturePackage(), input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Name, "plumber:derive")
	assert.Equal(t, result[0].Args[0], "Foo")
}

// TestExpandAnnotations_DefersTemplates verifies that the macro stage
// substitutes annotations verbatim (templates intact) and records the
// triggering annotation via ImpliedBy on each child.
func TestExpandAnnotations_DefersTemplates(t *testing.T) {
	macroMap := map[string]*config.PlumberMacroConfig{
		"@derive": {
			Name: "@derive",
			Annotations: []config.AnnotationConfig{
				{Name: "plumber:derive", Args: []string{`{{ index .Source.Args 0 }}Derived`}},
				{Name: "plumber:output", Args: []string{"generated.go"}},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@derive", []string{"Foo"}),
	}

	result, err := expandAnnotations(fixturePackage(), input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 2)
	// Templates remain unexpanded at the macro stage.
	assert.Equal(t, result[0].Name, "plumber:derive")
	assert.Equal(t, result[0].Args[0], `{{ index .Source.Args 0 }}Derived`)
	assert.Equal(t, result[1].Name, "plumber:output")
	assert.Equal(t, result[1].Args[0], "generated.go")
	// Both children record the triggering macro annotation.
	assert.Assert(t, result[0].ImpliedBy != nil)
	assert.Equal(t, result[0].ImpliedBy.Name, "@derive")
	assert.Assert(t, result[1].ImpliedBy != nil)
	assert.Equal(t, result[1].ImpliedBy.Name, "@derive")
}

func TestTransformerAnnotations_TemplateArgs(t *testing.T) {
	macroMap := map[string]*config.PlumberMacroConfig{
		"@derive": {
			Name: "@derive",
			Annotations: []config.AnnotationConfig{
				{Name: "plumber:derive", Args: []string{`{{ index .Source.Args 0 }}Derived`}},
				{Name: "plumber:output", Args: []string{"generated.go"}},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@derive", []string{"Foo"}),
	}

	result, err := expandAndTransform(t, fixtureNode(), input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 2)
	assert.Equal(t, result[0].Name, "plumber:derive")
	assert.Equal(t, result[0].Args[0], "FooDerived")
	assert.Equal(t, result[1].Name, "plumber:output")
	assert.Equal(t, result[1].Args[0], "generated.go")
}

func TestTransformerAnnotations_TemplateNamedArgs(t *testing.T) {
	macroMap := map[string]*config.PlumberMacroConfig{
		"@gen": {
			Name: "@gen",
			Annotations: []config.AnnotationConfig{
				{
					Name: "plumber:output",
					NamedArgs: map[string]string{
						"dir": `{{ .Source.NamedArgs.dir }}/generated`,
					},
				},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@gen", nil, model.WithNamedArgs(map[string]string{"dir": "out"})),
	}

	result, err := expandAndTransform(t, fixtureNode(), input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].NamedArgs["dir"], "out/generated")
}

// TestTransformerAnnotations_PackageContext verifies that .Package.Name and
// .Package.Path are exposed to the macro template context during the deferred
// transformer-stage expansion.
func TestTransformerAnnotations_PackageContext(t *testing.T) {
	macroMap := map[string]*config.PlumberMacroConfig{
		"@pkg": {
			Name: "@pkg",
			Annotations: []config.AnnotationConfig{
				{Name: "plumber:derive", Args: []string{`{{ .Package.Name }}Derived`}},
				{Name: "plumber:comment", Args: []string{`from {{ .Package.Path }}`}},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@pkg", nil),
	}

	result, err := expandAndTransform(t, fixtureNode(), input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 2)
	assert.Equal(t, result[0].Args[0], "fixtureDerived")
	assert.Equal(t, result[1].Args[0], "from example.com/fixture")
}

// TestTransformerAnnotations_TypeContext verifies that the AST node itself is
// exposed under .Type so templates may inspect type metadata such as the
// type's name or its source filename.
func TestTransformerAnnotations_TypeContext(t *testing.T) {
	macroMap := map[string]*config.PlumberMacroConfig{
		"@named": {
			Name: "@named",
			Annotations: []config.AnnotationConfig{
				{Name: "plumber:name", Args: []string{`{{ .Type.Name }}Filter`}},
				{Name: "plumber:comment", Args: []string{`source: {{ .Type.GetPosition.Filename }}`}},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@named", nil),
	}

	result, err := expandAndTransform(t, fixtureNode(), input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 2)
	assert.Equal(t, result[0].Args[0], "FixtureTypeFilter")
	assert.Equal(t, result[1].Args[0], "source: fixture.go")
}

// TestTransformerAnnotations_NilNode asserts that a nil node gracefully
// degrades to empty .Package fields and a nil .Type rather than panicking,
// as long as templates do not dereference .Type.
func TestTransformerAnnotations_NilNode(t *testing.T) {
	macroMap := map[string]*config.PlumberMacroConfig{
		"@pkg": {
			Name: "@pkg",
			Annotations: []config.AnnotationConfig{
				{Name: "plumber:derive", Args: []string{`prefix-{{ .Package.Name }}`}},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@pkg", nil),
	}

	result, err := expandAndTransform(t, nil, input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Args[0], "prefix-")
}

func TestTransformerAnnotations_TemplateError(t *testing.T) {
	macroMap := map[string]*config.PlumberMacroConfig{
		"@bad": {
			Name: "@bad",
			Annotations: []config.AnnotationConfig{
				{Name: "plumber:derive", Args: []string{`{{ .NonExistent }}`}},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@bad", nil),
	}

	_, err := expandAndTransform(t, fixtureNode(), input, macroMap)
	assert.ErrorContains(t, err, "expanding implied annotation")
}

func TestTransformerAnnotations_NoTemplatePassthrough(t *testing.T) {
	macroMap := map[string]*config.PlumberMacroConfig{
		"@simple": {
			Name: "@simple",
			Annotations: []config.AnnotationConfig{
				{Name: "plumber:derive", Args: []string{"{name}Macro"}},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@simple", nil),
	}

	result, err := expandAndTransform(t, fixtureNode(), input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	// No {{ }} in the string, so it passes through unchanged.
	assert.Equal(t, result[0].Args[0], "{name}Macro")
}

// TestTransformerAnnotations_PassthroughWithoutImpliedBy verifies that
// annotations not implied by another annotation are returned unchanged even
// when their args contain template syntax (templates are only expanded on
// implied annotations).
func TestTransformerAnnotations_PassthroughWithoutImpliedBy(t *testing.T) {
	input := model.Annotations{
		model.NewAnnotation("plumber:derive", []string{`{{ .Source.Args 0 }}`}),
	}
	result, err := TransformerAnnotations(fixtureNode(), input, render.Scope{})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Args[0], `{{ .Source.Args 0 }}`)
}
