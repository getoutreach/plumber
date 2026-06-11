// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the handler configuration types for the shape command notification system.

package config

// HandlerConfig represents a handler entry in the shape configuration that maps
// a handler name to a command template. When transformers emit plumber:notify
// annotations targeting a handler by name, the corresponding command template
// is expanded with the aggregated named arguments and executed via sh -c.
type HandlerConfig struct {
	PlumberHandler *PlumberHandlerConfig `yaml:"plumber.handler,omitempty"`
}

// PlumberHandlerConfig holds the name and command template for a single handler.
type PlumberHandlerConfig struct {
	// Doc is an optional documentation for the handler, which can be used in generated documentation or help output.
	Doc DocumentationConfig `yaml:"doc,omitempty"`

	// Name is the handler identifier that plumber:notify annotations reference.
	Name string `yaml:"name"`
	// Command is a Go template string that is expanded with the aggregated
	// named arguments from all notifications targeting this handler.
	// The template has access to .Source.NamedArgs (map[string][]string)
	// and standard sprig + plumber template functions.
	Command string `yaml:"command"`
	// Args is an optional list of additional arguments to pass to the command, which can also be templated.
	Args []string `yaml:"args,omitempty"`
	// Variants is an optional list of command variants that can be selected based on additional criteria,
	// allowing for more flexible handler behavior.
	Variants []HandlerVariantConfig `yaml:"variants,omitempty"`
}

// HandlerVariantConfig represents a variant of a handler that can be used in the shape configuration,
type HandlerVariantConfig struct {
	Name    string   `yaml:"name"`
	Command string   `yaml:"command"`
	Args    []string `yaml:"args,omitempty"`
}
