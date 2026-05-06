// Copyright 2026 Outreach Corporation. All Rights Reserved.
// Description: This file defines the structures configuration for the shape command.
package config

// StructureConfig represents the configuration for a structure in the shape command,
type StructureConfig struct {
	Structure PlumberStructureConfig `yaml:"plumber.structure,omitempty"`
}

// PlumberStructureConfig represents the configuration for a structure in the shape command,
type PlumberStructureConfig struct {
	Name        string                `yaml:"name"`
	Description string                `yaml:"description,omitempty"`
	Path        string                `yaml:"path,omitempty"`
	Paths       []StructurePathConfig `yaml:"paths,omitempty"`
}

// StructurePathConfig represents the configuration for a structure path in the shape command,
type StructurePathConfig struct {
	Path PlumberStructurePathConfig `yaml:"plumber.path,omitempty"`
}

// PlumberStructurePathConfig represents the configuration for a structure path in the shape command,
type PlumberStructurePathConfig struct {
	Name               string `yaml:"name"`
	Description        string `yaml:"description,omitempty"`
	PackageDescription string `yaml:"package_description,omitempty"`
	Path               string `yaml:"path,omitempty"`
	Template           string `yaml:"template,omitempty"`
}
