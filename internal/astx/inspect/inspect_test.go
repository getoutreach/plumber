// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains tests for annotation and struct tag parsing.

package inspect

import (
	"testing"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/query/model"
	"gotest.tools/v3/assert"

	_ "github.com/santhosh-tekuri/jsonschema"
)

func TestParseAnnotations(t *testing.T) {
	doc := `lorem ipsum dolor sit amet
    lorem.ipsum dolor sit amet lorem ipsum dolor sit amet

    plumber.annotation arg1 arg2 key1=value1 key2=value2
    plumber:annotation arg1 key2=value2
    @plumber.skip arg1 key2=value2

    goverter.context yes
    `
	p := model.Position{
		Line: 2,
	}

	cg := &model.CommentGroup{
		Position: p,
	}

	assert.DeepEqual(t, []model.Annotation{
		{
			Name: "plumber.annotation",
			Args: []string{"arg1", "arg2"},
			NamedArgs: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			DocLine:  3,
			Position: &model.Position{Line: 5},
			Source:   cg,
		},
		{
			Name: "plumber:annotation",
			Args: []string{"arg1"},
			NamedArgs: map[string]string{
				"key2": "value2",
			},
			DocLine:  4,
			Position: &model.Position{Line: 6},
			Source:   cg,
		},
		{
			Name: "@plumber.skip",
			Args: []string{"arg1"},
			NamedArgs: map[string]string{
				"key2": "value2",
			},
			DocLine:  5,
			Position: &model.Position{Line: 7},
			Source:   cg,
		},
		{
			Name:      "goverter.context",
			Args:      []string{"yes"},
			NamedArgs: map[string]string{},
			DocLine:   6,
			Position:  &model.Position{Line: 8},
			Source:    cg,
		},
	}, ParseAnnotations(doc, cg))
}

func TestParseTags(t *testing.T) {
	tag := `json:"name,omitempty" yaml:"name,omitempty" validate:"required"`

	assert.DeepEqual(t, []model.Tag{
		{
			Name:  "json",
			Value: "name,omitempty",
		},
		{
			Name:  "yaml",
			Value: "name,omitempty",
		},
		{
			Name:  "validate",
			Value: "required",
		},
	}, ParseTags(tag))
}

func TestAnnotationsFromDecs(t *testing.T) {
	decs := dst.Decorations{
		"// generate:once",
		"// plumber:provider",
	}

	annotations := AnnotationsFromDecs(decs)
	assert.Equal(t, len(annotations), 2)
	assert.Equal(t, annotations[0].Name, "generate:once")
	assert.Equal(t, annotations[1].Name, "plumber:provider")
}

func TestAnnotationsFromDecs_Empty(t *testing.T) {
	annotations := AnnotationsFromDecs(dst.Decorations{})
	assert.Equal(t, len(annotations), 0)
}

func TestAnnotationsFromDecs_NonAnnotationComments(t *testing.T) {
	decs := dst.Decorations{
		"// This is a regular comment",
	}

	annotations := AnnotationsFromDecs(decs)
	assert.Equal(t, len(annotations), 0)
}

func TestAnnotationsFromDecs_MultipleDecs(t *testing.T) {
	decs1 := dst.Decorations{"// generate:once"}
	decs2 := dst.Decorations{"// plumber:provider"}

	annotations := AnnotationsFromDecs(decs1, decs2)
	assert.Equal(t, len(annotations), 2)
	assert.Equal(t, annotations[0].Name, "generate:once")
	assert.Equal(t, annotations[1].Name, "plumber:provider")
}

func TestParseAnnotations_Quoted(t *testing.T) {
	p := model.Position{}

	cg := &model.CommentGroup{
		Position: p,
	}

	tests := []struct {
		name string
		doc  string
		want []model.Annotation
	}{
		{
			name: "double-quoted arg with spaces",
			doc:  `plumber.foo "X Y"`,
			want: []model.Annotation{{
				Name:      "plumber.foo",
				Args:      []string{"X Y"},
				NamedArgs: map[string]string{},
				DocLine:   1,
				Position:  &p,
				Source:    cg,
			}},
		},
		{
			name: "single-quoted arg with spaces",
			doc:  `plumber.foo 'hello world'`,
			want: []model.Annotation{{
				Name:      "plumber.foo",
				Args:      []string{"hello world"},
				NamedArgs: map[string]string{},
				DocLine:   1,
				Position:  &p,
				Source:    cg,
			}},
		},
		{
			name: "backtick-quoted arg with spaces",
			doc:  "plumber.foo `hello world`",
			want: []model.Annotation{{
				Name:      "plumber.foo",
				Args:      []string{"hello world"},
				NamedArgs: map[string]string{},
				DocLine:   1,
				Position:  &p,
				Source:    cg,
			}},
		},
		{
			name: "quotes not fully wrapping are preserved",
			doc:  `plumber.foo "X".value`,
			want: []model.Annotation{{
				Name:      "plumber.foo",
				Args:      []string{`"X".value`},
				NamedArgs: map[string]string{},
				DocLine:   1,
				Position:  &p,
				Source:    cg,
			}},
		},
		{
			name: "named arg with quoted value",
			doc:  `plumber.foo key="val with space"`,
			want: []model.Annotation{{
				Name:      "plumber.foo",
				Args:      nil,
				NamedArgs: map[string]string{"key": "val with space"},
				DocLine:   1,
				Position:  &p,
				Source:    cg,
			}},
		},
		{
			name: "mixed quoted and unquoted args",
			doc:  `plumber.foo arg1 "arg two" key='val ue'`,
			want: []model.Annotation{{
				Name:      "plumber.foo",
				Args:      []string{"arg1", "arg two"},
				NamedArgs: map[string]string{"key": "val ue"},
				DocLine:   1,
				Position:  &p,
				Source:    cg,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseAnnotations(tt.doc, cg)
			assert.DeepEqual(t, tt.want, got)
		})
	}
}
