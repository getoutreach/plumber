// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines annotation option constants used by the shape command.

// Package contract defines the annotation constants for the plumber shape command.
package contract

// Option constants for annotations used in plumber templates and code generation.
const (
	// OptionTemplate specifies the template to use for code generation.
	OptionTemplate = "plumber:template"
	// OptionIgnore specifies a template to ignore, allowing for selective exclusion of templates during code generation.
	OptionIgnore = "plumber:ignore"
	// OptionContext specifies the context to pass to the template during code generation,
	// enabling dynamic behavior based on the node's context.
	OptionContext = "plumber:context"
	// OptionComment allows adding a comment to the generated code, which can be used for documentation or clarification purposes.
	OptionComment = "plumber:comment"
	// OptionName specifies a custom name for the generated type or function, overriding the default naming convention.
	OptionName = "plumber:name"
	// OptionReceiver specifies the receiver type for generated methods, allowing for method generation on specific types.
	OptionReceiver = "plumber:receiver"
	// OptionFilter specifies a filter to apply to the node's fields or methods,
	// enabling selective code generation based on field or method names, types, or other criteria.
	OptionFilter = "plumber:filter"
	// OptionMixin specifies a mixin template to include in the generated code,
	// allowing for code reuse and composition across different templates.
	OptionMixin = "plumber:mixin"
	// OptionOutput specifies the output path for the generated code, allowing for flexible file organization and output management.
	OptionOutput = "plumber:output"
	// OptionMode specifies the mode of code generation, such as "generate" for
	// generating new files or "inplace" for modifying existing files in place.
	OptionMode = "plumber:mode"
	// OptionFieldWrapper specifies a template to wrap struct fields, enabling custom
	// field-level code generation such as adding tags, validation, or other field-specific logic.
	OptionFieldWrapper = "plumber:field_wrapper"
	// OptionQuery specifies a query entry point that searches for entities matching a regex pattern
	// within a defined scope and populates an annotated slice variable with compatible results.
	OptionQuery = "plumber:query"
	// OptionScope injects a resolved type into the template scope under .Scope.Custom.<name>,
	// allowing templates to access additional type information beyond the subject type.
	// Usage: plumber:scope "MyType" type="pkg/path".TypeName
	OptionScope = "plumber:scope"
)
