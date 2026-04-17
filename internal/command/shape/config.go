// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines configuration types for the shape command including template, mixin, wrapper, and include settings.

package shape

import (
	"github.com/getoutreach/plumber/internal/command/inspect"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
)

// FileConfig represents the overall configuration for the shape command,
// including settings for templates, mixins, wrappers, and includes.
type FileConfig struct {
	Shape    Config             `yaml:"plumber.shape"`
	Inspect  inspect.FileConfig `yaml:"plumber.inspect"`
	Includes []IncludeConfig    `yaml:"includes,omitempty"`
}

// Config holds specific configuration options for the shape command, such as working directory,
// cache directory, template sources, mixins, and type wrappers.
type Config struct {
	WorkingDir string                                 `yaml:"workingDir,omitempty"`
	CacheDir   string                                 `yaml:"cacheDir,omitempty"`
	Sources    []contract.PlumberTemplateSourceConfig `yaml:"sources,omitempty"`
	Templates  contract.PlumberTemplatesConfig        `yaml:"templates,omitempty"`
	Macros     []MacroConfig                          `yaml:"macros,omitempty"`
	Mixins     []MixinConfig                          `yaml:"mixins,omitempty"`
	Type       TypeConfig                             `yaml:"type,omitempty"`
}

// TypeConfig represents the configuration for type transformations in the shape command,
// including wrappers that can be applied to struct fields or types.
type TypeConfig struct {
	Wrappers []WrapperConfig `yaml:"wrappers,omitempty"`
}

// WrapperConfig represents the configuration for a wrapper that can be applied to struct fields or types during code generation,
// specifying the name of the wrapper and the expressions that define how the wrapper should be applied.
type WrapperConfig struct {
	PlumberWrapper *PlumberWrapperConfig `yaml:"plumber.wrapper,omitempty"`
}

// PlumberWrapperConfig represents the configuration for a wrapper that can be applied to struct fields or types during code generation,
// specifying the name of the wrapper and the expressions that define how the wrapper should be applied.
type PlumberWrapperConfig struct {
	Name        string                    `yaml:"name"`
	Expressions []WrapperExpressionConfig `yaml:"expressions,omitempty"`
}

// WrapperExpressionConfig represents the configuration for a wrapper expression,
// which defines how a wrapper should be applied to struct fields or types during code generation,
type WrapperExpressionConfig struct {
	PlumberWrapperExpression *PlumberWrapperExpressionConfig `yaml:"plumber.wrapper_expression,omitempty"`
}

// PlumberWrapperExpressionConfig represents the configuration for a wrapper expression,
type PlumberWrapperExpressionConfig struct {
	Type    string            `yaml:"type"`
	Matches []MatchRuleConfig `yaml:"matches,omitempty"`
}

// MatchRuleConfig represents a configuration for a match rule,
// which defines criteria for matching struct fields or types during wrapper application in code generation.
type MatchRuleConfig struct {
	Rule string `yaml:"rule"`
}

// IncludeConfig represents the configuration for including additional shape configurations,
type IncludeConfig struct {
	Path string `yaml:"path"`
}

// MixinConfig represents the configuration for mixins that can be included in the shape command,
type MixinConfig struct {
	PlumberMixin *PlumberMixinConfig `yaml:"plumber.mixin,omitempty"`
}

// PlumberMixinConfig represents the configuration for mixins that can be included in the shape command,
type PlumberMixinConfig struct {
	Name        string             `yaml:"name"`
	Annotations []AnnotationConfig `yaml:"annotations,omitempty"`
}

// MacroConfig represents the configuration for a macro that expands into a set of annotations
// before transformer building, allowing injection of any annotation including entry-point annotations like plumber:derive.
type MacroConfig struct {
	PlumberMacro *PlumberMacroConfig `yaml:"plumber.macro,omitempty"`
}

// PlumberMacroConfig represents the configuration for a macro, specifying its name and the annotations
// it expands into when referenced in Go source comments.
type PlumberMacroConfig struct {
	Name        string             `yaml:"name"`
	Annotations []AnnotationConfig `yaml:"annotations,omitempty"`
}

// AnnotationConfig represents a configuration for filtering nodes based on specific annotation names.
type AnnotationConfig struct {
	Name      string            `yaml:"name"`
	Args      []string          `yaml:"args,omitempty"`
	NamedArgs map[string]string `yaml:"namedArgs,omitempty"`
}

func (c *FileConfig) Merge(includes ...*FileConfig) {
	for _, include := range includes {
		c.Shape.MergeShape(&include.Shape)
	}
}

// MergeShape merges another Config into this one, appending sources, templates, mixins, and wrappers.
func (c *Config) MergeShape(other *Config) {
	c.Sources = append(c.Sources, other.Sources...)
	c.Templates.Content = append(c.Templates.Content, other.Templates.Content...)
	c.Macros = append(c.Macros, other.Macros...)
	c.Mixins = append(c.Mixins, other.Mixins...)
	c.Type.Wrappers = append(c.Type.Wrappers, other.Type.Wrappers...)
}
