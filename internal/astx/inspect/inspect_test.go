// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains tests for annotation and struct tag parsing.

package inspect

import (
	"testing"

	"github.com/getoutreach/plumber/query/model"
	"gotest.tools/v3/assert"
)

func TestParseAnnotations(t *testing.T) {
	doc := `
    lorem ipsum dolor sit amet
    lorem.ipsum dolor sit amet lorem ipsum dolor sit amet

    plumber.annotation arg1 arg2 key1=value1 key2=value2
    plumber:annotation arg1 key2=value2
    @plumber.skip arg1 key2=value2

    goverter.context yes
    `

	assert.DeepEqual(t, []model.Annotation{
		{
			Name: "plumber.annotation",
			Args: []string{"arg1", "arg2"},
			NamedArgs: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
		{
			Name: "plumber:annotation",
			Args: []string{"arg1"},
			NamedArgs: map[string]string{
				"key2": "value2",
			},
		},
		{
			Name: "@plumber.skip",
			Args: []string{"arg1"},
			NamedArgs: map[string]string{
				"key2": "value2",
			},
		},
		{
			Name:      "goverter.context",
			Args:      []string{"yes"},
			NamedArgs: map[string]string{},
		},
	}, ParseAnnotations(doc))
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
