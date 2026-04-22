// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains unit tests for macro template expansion in expandAnnotations.

package expand

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape/config"
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

func TestExpandAnnotations_TemplateArgs(t *testing.T) {
	macroMap := map[string]*config.PlumberMacroConfig{
		"@derive": {
			Name: "@derive",
			Annotations: []config.AnnotationConfig{
				{Name: "plumber:derive", Args: []string{`{{ index .Macro.Args 0 }}Derived`}},
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
	assert.Equal(t, result[0].Name, "plumber:derive")
	assert.Equal(t, result[0].Args[0], "FooDerived")
	assert.Equal(t, result[1].Name, "plumber:output")
	assert.Equal(t, result[1].Args[0], "generated.go")
}

func TestExpandAnnotations_TemplateNamedArgs(t *testing.T) {
	macroMap := map[string]*config.PlumberMacroConfig{
		"@gen": {
			Name: "@gen",
			Annotations: []config.AnnotationConfig{
				{
					Name: "plumber:output",
					NamedArgs: map[string]string{
						"dir": `{{ .Macro.NamedArgs.dir }}/generated`,
					},
				},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@gen", nil, model.WithNamedArgs(map[string]string{"dir": "out"})),
	}

	result, err := expandAnnotations(fixturePackage(), input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].NamedArgs["dir"], "out/generated")
}

// TestExpandAnnotations_PackageContext verifies that .Package.Name and
// .Package.Path are exposed to the macro template context.
func TestExpandAnnotations_PackageContext(t *testing.T) {
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

	result, err := expandAnnotations(fixturePackage(), input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 2)
	assert.Equal(t, result[0].Args[0], "fixtureDerived")
	assert.Equal(t, result[1].Args[0], "from example.com/fixture")
}

// TestExpandAnnotations_NilPackage asserts that a nil package gracefully degrades
// to empty .Package.Name / .Package.Path values rather than panicking.
func TestExpandAnnotations_NilPackage(t *testing.T) {
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

	result, err := expandAnnotations(nil, input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Args[0], "prefix-")
}

func TestExpandAnnotations_TemplateError(t *testing.T) {
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

	_, err := expandAnnotations(fixturePackage(), input, macroMap)
	assert.ErrorContains(t, err, "expanding macro")
}

func TestExpandAnnotations_NoTemplatePassthrough(t *testing.T) {
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

	result, err := expandAnnotations(fixturePackage(), input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	// No {{ }} in the string, so it passes through unchanged.
	assert.Equal(t, result[0].Args[0], "{name}Macro")
}
