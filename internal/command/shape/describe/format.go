// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the pluggable Formatter interface and a registry for output format implementations.

package describe

import (
	"fmt"
	"strings"
)

// Formatter renders a Description into a specific output format.
type Formatter interface {
	Format(desc Description) ([]byte, error)
}

// Formats
const (
	// FormatJSON constants for registry keys and user-facing documentation.
	FormatJSON = "json"
	// FormatYAML renders descriptions in YAML format, which is more human-readable than JSON and supports comments in some renderers.
	FormatYAML = "yaml"
	// FormatMD renders descriptions in Markdown format, which is ideal for documentation and skill templates.
	FormatMD = "md"
)

// Get returns the formatter registered under the given name, or an error if
// no formatter with that name exists.
func Format(name string) (Formatter, error) {
	switch name {
	case FormatJSON:
		return jsonFormatter{}, nil
	case FormatYAML:
		return yamlFormatter{}, nil
	case FormatMD:
		return mdFormatter{}, nil
	default:
		return nil, fmt.Errorf("unknown format %q; available: %s", name, strings.Join(Formats(), ", "))
	}
}

// Formats returns the names of all registered formatters.
func Formats() []string {
	return []string{FormatJSON, FormatYAML, FormatMD}
}
