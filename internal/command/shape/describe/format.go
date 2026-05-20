// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the pluggable Formatter interface and a registry for output format implementations.

package describe

import "fmt"

// Formatter renders a Description into a specific output format.
type Formatter interface {
	Format(desc Description) ([]byte, error)
}

// Get returns the formatter registered under the given name, or an error if
// no formatter with that name exists.
func Format(name string) (Formatter, error) {
	switch name {
	case "json":
		return jsonFormatter{}, nil
	case "yaml":
		return yamlFormatter{}, nil
	case "md":
		return mdFormatter{}, nil
	default:
		return nil, fmt.Errorf("unknown format %q; available: json, yaml, md", name)
	}
}

// Formats returns the names of all registered formatters.
func Formats() []string {
	return []string{"json", "yaml", "md"}
}
