// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines configuration types for the shape command including template, mixin, wrapper, and include settings.

package shape

import (
	"github.com/getoutreach/plumber/internal/command/inspect"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
)

type Config struct {
	Shape    ShapeConfig     `yaml:"plumber.shape"`
	Inspect  inspect.Config  `yaml:"plumber.inspect"`
	Includes []IncludeConfig `yaml:"includes,omitempty"`
}

type ShapeConfig struct {
	WorkingDir string                          `yaml:"workingDir,omitempty"`
	CacheDir   string                          `yaml:"cacheDir,omitempty"`
	Templates  contract.PlumberTemplatesConfig `yaml:"templates,omitempty"`
	Mixins     []MixinConfig                   `yaml:"mixins,omitempty"`
	Type       TypeConfig                      `yaml:"type,omitempty"`
}

type TypeConfig struct {
	Wrappers []WrapperConfig `yaml:"wrappers,omitempty"`
}

type WrapperConfig struct {
	PlumberWrapper *PlumberWrapperConfig `yaml:"plumber.wrapper,omitempty"`
}

type PlumberWrapperConfig struct {
	Name        string                    `yaml:"name"`
	Expressions []WrapperExpressionConfig `yaml:"expressions,omitempty"`
}

type WrapperExpressionConfig struct {
	PlumberWrapperExpression *PlumberWrapperExpressionConfig `yaml:"plumber.wrapper_expression,omitempty"`
}

type PlumberWrapperExpressionConfig struct {
	Type    string            `yaml:"type"`
	Matches []MatchRuleConfig `yaml:"matches,omitempty"`
}

type MatchRuleConfig struct {
	Rule string `yaml:"rule"`
}

type IncludeConfig struct {
	Path string `yaml:"path"`
}

type MixinConfig struct {
	PlumberMixin *PlumberMixinConfig `yaml:"plumber.mixin,omitempty"`
}

type PlumberMixinConfig struct {
	Name        string             `yaml:"name"`
	Annotations []AnnotationConfig `yaml:"annotations,omitempty"`
}

type AnnotationConfig struct {
	Name      string            `yaml:"name"`
	Args      []string          `yaml:"args,omitempty"`
	NamedArgs map[string]string `yaml:"namedArgs,omitempty"`
}

func (c *Config) Merge(includes ...*Config) {
	for _, include := range includes {
		c.Shape.Templates.Sources = append(c.Shape.Templates.Sources, include.Shape.Templates.Sources...)
		c.Shape.Templates.Content = append(c.Shape.Templates.Content, include.Shape.Templates.Content...)
		c.Shape.Mixins = append(c.Shape.Mixins, include.Shape.Mixins...)
		c.Shape.Type.Wrappers = append(c.Shape.Type.Wrappers, include.Shape.Type.Wrappers...)
	}
}
