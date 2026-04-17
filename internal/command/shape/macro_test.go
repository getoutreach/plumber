// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains unit tests for macro template expansion in expandAnnotations.

package shape

import (
	"testing"

	"github.com/getoutreach/plumber/query/model"
	"gotest.tools/v3/assert"
)

func TestExpandAnnotations_NoMacro(t *testing.T) {
	macroMap := map[string]*PlumberMacroConfig{}
	input := model.Annotations{
		model.NewAnnotation("plumber:derive", []string{"Foo"}),
	}

	result, err := expandAnnotations(input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Name, "plumber:derive")
	assert.Equal(t, result[0].Args[0], "Foo")
}

func TestExpandAnnotations_TemplateArgs(t *testing.T) {
	macroMap := map[string]*PlumberMacroConfig{
		"@derive": {
			Name: "@derive",
			Annotations: []AnnotationConfig{
				{Name: "plumber:derive", Args: []string{`{{ index .Args 0 }}Derived`}},
				{Name: "plumber:output", Args: []string{"generated.go"}},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@derive", []string{"Foo"}),
	}

	result, err := expandAnnotations(input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 2)
	assert.Equal(t, result[0].Name, "plumber:derive")
	assert.Equal(t, result[0].Args[0], "FooDerived")
	assert.Equal(t, result[1].Name, "plumber:output")
	assert.Equal(t, result[1].Args[0], "generated.go")
}

func TestExpandAnnotations_TemplateNamedArgs(t *testing.T) {
	macroMap := map[string]*PlumberMacroConfig{
		"@gen": {
			Name: "@gen",
			Annotations: []AnnotationConfig{
				{
					Name: "plumber:output",
					NamedArgs: map[string]string{
						"dir": `{{ .NamedArgs.dir }}/generated`,
					},
				},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@gen", nil, model.WithNamedArgs(map[string]string{"dir": "out"})),
	}

	result, err := expandAnnotations(input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].NamedArgs["dir"], "out/generated")
}

func TestExpandAnnotations_TemplateError(t *testing.T) {
	macroMap := map[string]*PlumberMacroConfig{
		"@bad": {
			Name: "@bad",
			Annotations: []AnnotationConfig{
				{Name: "plumber:derive", Args: []string{`{{ .NonExistent }}`}},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@bad", nil),
	}

	_, err := expandAnnotations(input, macroMap)
	assert.ErrorContains(t, err, "expanding macro")
}

func TestExpandAnnotations_NoTemplatePassthrough(t *testing.T) {
	macroMap := map[string]*PlumberMacroConfig{
		"@simple": {
			Name: "@simple",
			Annotations: []AnnotationConfig{
				{Name: "plumber:derive", Args: []string{"{name}Macro"}},
			},
		},
	}

	input := model.Annotations{
		model.NewAnnotation("@simple", nil),
	}

	result, err := expandAnnotations(input, macroMap)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	// No {{ }} in the string, so it passes through unchanged.
	assert.Equal(t, result[0].Args[0], "{name}Macro")
}
