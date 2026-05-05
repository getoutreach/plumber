// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines configuration structures and functions for the shape command.

// Package config provides functionality for managing configuration settings for the shape command.
package config

// TargetConfig holds parameters for single-type targeted mode where a specific type is
// processed with a named macro, bypassing the full annotation scan.
type TargetConfig struct {
	TypeFQN   string            // fully-qualified type name
	Macro     string            // macro name (e.g. "@derive")
	Args      []string          // positional macro args
	NamedArgs map[string]string // named macro args (key=value)
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
	Matcher string            `yaml:"matcher,omitempty"`
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

// MatcherConfig represents a named, reusable matcher defined at plumber.shape.matchers.
type MatcherConfig struct {
	PlumberMatcher *PlumberMatcherConfig `yaml:"plumber.matcher,omitempty"`
}

// PlumberMatcherConfig holds the name and match rules for a reusable matcher
// that can be referenced by name from wrapper expressions.
type PlumberMatcherConfig struct {
	Name    string            `yaml:"name"`
	Matches []MatchRuleConfig `yaml:"matches,omitempty"`
}
