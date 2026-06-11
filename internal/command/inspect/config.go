// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines configuration types for the inspect command, including format and annotation settings.

package inspect

// FileConfig represents the overall configuration for the inspect command, including the format and annotation settings.
type FileConfig struct {
	Inspect *Config `yaml:"plumber.inspect"`
}

// Config holds specific configuration options for the inspect command, such as output format and annotation filters.
type Config struct {
	Format            string              `yaml:"format,omitempty"`
	AnnotationsConfig []AnnotationsConfig `yaml:"annotations,omitempty"`
}

// AnnotationsConfig represents a configuration for filtering nodes based on specific annotation names.
type AnnotationsConfig struct {
	List []string `yaml:"list,omitempty"`
}
