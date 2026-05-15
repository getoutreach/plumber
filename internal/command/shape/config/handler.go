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
	// Name is the handler identifier that plumber:notify annotations reference.
	Name string `yaml:"name"`
	// Command is a Go template string that is expanded with the aggregated
	// named arguments from all notifications targeting this handler.
	// The template has access to .Source.NamedArgs (map[string][]string)
	// and standard sprig + plumber template functions.
	Command string `yaml:"command"`
}
