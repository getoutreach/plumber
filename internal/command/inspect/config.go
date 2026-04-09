// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines configuration types for the inspect command, including format and annotation settings.

package inspect

type Config struct {
	Inspect *InspectConfig `yaml:"plumber.inspect"`
}

type InspectConfig struct {
	Format            string              `yaml:"format,omitempty"`
	AnnotationsConfig []AnnotationsConfig `yaml:"annotations,omitempty"`
}

type AnnotationsConfig struct {
	List []string `yaml:"list,omitempty"`
}
