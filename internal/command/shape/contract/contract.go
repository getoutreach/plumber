// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines annotation option constants and template configuration types used by the shape command.

// Package contract defines the annotation constants and template configuration types for the plumber shape command.
package contract

const (
	OptionTemplate     = "plumber:template"
	OptionIgnore       = "plumber:ignore"
	OptionContext      = "plumber:context"
	OptionComment      = "plumber:comment"
	OptionName         = "plumber:name"
	OptionReceiver     = "plumber:receiver"
	OptionFilter       = "plumber:filter"
	OptionMixin        = "plumber:mixin"
	OptionOutput       = "plumber:output"
	OptionMode         = "plumber:mode"
	OptionFieldWrapper = "plumber:field_wrapper"
)

type (
	PlumberTemplatesConfig struct {
		Sources []PlumberTemplateSourceConfig  `yaml:"sources,omitempty"`
		Content []PlumberTemplateContentConfig `yaml:"content,omitempty"`
	}

	PlumberTemplateSourceConfig struct {
		Local *PlumberTemplateLocalSourceConfig `yaml:"local,omitempty"`
		Git   *PlumberTemplateGitSourceConfig   `yaml:"git,omitempty"`
	}

	PlumberTemplateLocalSourceConfig struct {
		Path      string                  `yaml:"path"`
		Templates []PlumberTemplateConfig `yaml:"templates,omitempty"`
	}

	PlumberTemplateGitSourceConfig struct {
		Repository string                  `yaml:"repository"`
		Ref        string                  `yaml:"ref,omitempty"`
		Templates  []PlumberTemplateConfig `yaml:"templates,omitempty"`
	}

	PlumberTemplateConfig struct {
		Name string `yaml:"name"`
		Path string `yaml:"path,omitempty"`
	}

	PlumberTemplateContentConfig struct {
		Name    string `yaml:"name"`
		Content string `yaml:"content"`
	}
)
