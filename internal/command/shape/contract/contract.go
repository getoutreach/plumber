// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines annotation option constants and template configuration types used by the shape command.

// Package contract defines the annotation constants and template configuration types for the plumber shape command.
package contract

// Option constants for annotations used in plumber templates and code generation.
const (
	// OptionTemplate specifies the template to use for code generation.
	OptionTemplate = "plumber:template"
	// OptionDerive specifies a template to derive from, allowing for template composition and reuse.
	OptionIgnore = "plumber:ignore"
	// OptionContext specifies the context to pass to the template during code generation, enabling dynamic behavior based on the node's context.
	OptionContext = "plumber:context"
	// OptionComment allows adding a comment to the generated code, which can be used for documentation or clarification purposes.
	OptionComment = "plumber:comment"
	// OptionName specifies a custom name for the generated type or function, overriding the default naming convention.
	OptionName = "plumber:name"
	// OptionReceiver specifies the receiver type for generated methods, allowing for method generation on specific types.
	OptionReceiver = "plumber:receiver"
	// OptionFilter specifies a filter to apply to the node's fields or methods, enabling selective code generation based on field or method names, types, or other criteria.
	OptionFilter = "plumber:filter"
	// OptionMixin specifies a mixin template to include in the generated code, allowing for code reuse and composition across different templates.
	OptionMixin = "plumber:mixin"
	// OptionOutput specifies the output path for the generated code, allowing for flexible file organization and output management.
	OptionOutput = "plumber:output"
	// OptionMode specifies the mode of code generation, such as "generate" for generating new files or "inplace" for modifying existing files in place.
	OptionMode = "plumber:mode"
	// OptionFieldWrapper specifies a template to wrap struct fields, enabling custom field-level code generation such as adding tags, validation, or other field-specific logic.
	OptionFieldWrapper = "plumber:field_wrapper"
	// OptionQuery specifies a query entry point that searches for entities matching a regex pattern
	// within a defined scope and populates an annotated slice variable with compatible results.
	OptionQuery = "plumber:query"
)

// Types
type (
	// PlumberTemplatesConfig represents the overall configuration for plumber templates content.
	PlumberTemplatesConfig struct {
		Content []PlumberTemplateContentConfig `yaml:"content,omitempty"`
	}

	// PlumberTemplateSourceConfig represents a source of templates, which can be either local or from a Git repository.
	PlumberTemplateSourceConfig struct {
		Local *PlumberTemplateLocalSourceConfig `yaml:"local,omitempty"`
		Git   *PlumberTemplateGitSourceConfig   `yaml:"git,omitempty"`
	}

	// PlumberTemplateLocalSourceConfig represents a local source of templates, specifying the path to the templates and the templates to use.
	PlumberTemplateLocalSourceConfig struct {
		Path      string                  `yaml:"path"`
		Templates []PlumberTemplateConfig `yaml:"templates,omitempty"`
	}

	// PlumberTemplateGitSourceConfig represents a Git source of templates, specifying the repository, reference, and templates to use.
	// It also supports includes for loading additional configuration files from the Git repository.
	PlumberTemplateGitSourceConfig struct {
		Repository string                    `yaml:"repository"`
		Ref        string                    `yaml:"ref,omitempty"`
		Includes   []PlumberGitIncludeConfig `yaml:"includes,omitempty"`
		Templates  []PlumberTemplateConfig   `yaml:"templates,omitempty"`
	}

	// PlumberGitIncludeConfig represents an include path for loading additional configuration files
	// from within a Git repository source, supporting glob patterns relative to the repository root.
	PlumberGitIncludeConfig struct {
		Path string `yaml:"path"`
	}

	// PlumberTemplateConfig represents a single template configuration,
	// specifying the name of the template and an optional path to the template file.
	PlumberTemplateConfig struct {
		Name string `yaml:"name"`
		Path string `yaml:"path,omitempty"`
	}

	// PlumberTemplateContentConfig represents a template configuration that includes the content of the template directly,
	// allowing for inline template definitions without needing a separate file.
	PlumberTemplateContentConfig struct {
		Name    string `yaml:"name"`
		Content string `yaml:"content"`
	}
)
