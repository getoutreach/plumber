// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Configuration models for plumber discovery
// Managed: true

package discovery

// Config represents the root configuration for plumber discovery
type Config struct {
	Applications []Application   `yaml:"applications"`
	Templates    TemplatesConfig `yaml:"templates,omitempty"`
}

// TemplatesConfig contains template definitions
type TemplatesConfig struct {
	Container string `yaml:"container,omitempty"`
}

// Application represents a single application module
type Application struct {
	Name       string      `yaml:"name"`
	Module     string      `yaml:"module,omitempty"`
	Config     string      `yaml:"config,omitempty"`
	Containers []Container `yaml:"containers"`
}

// Container represents a plumber sub-container configuration
// The configuration uses ansible-like syntax where the type is the key
type Container struct {
	// Type-specific configurations
	PlumberContainer *PlumberContainerConfig `yaml:"plumber.container,omitempty"`
	Loop             *LoopConfig             `yaml:"loop,omitempty"`
}

// PlumberContainerConfig contains the configuration for a plumber.container type
type PlumberContainerConfig struct {
	Comment   string              `yaml:"comment,omitempty"`
	Name      string              `yaml:"name"`
	Container ContainerPathConfig `yaml:"container"`
	Source    *SourcePathConfig   `yaml:"source,omitempty"`
	Matchers  []Matcher           `yaml:"matchers,omitempty"`
}

// ContainerPathConfig specifies the path to the container definition
type ContainerPathConfig struct {
	Path string `yaml:"path"`
}

// SourcePathConfig specifies the path to source files for discovery
type SourcePathConfig struct {
	Path string `yaml:"path"`
}

// Matcher represents a matcher configuration
type Matcher struct {
	// Constructor patterns with optional named capture groups
	Constructors []string `yaml:"constructors,omitempty"`
}

// LoopConfig defines how to iterate over paths to create multiple containers
type LoopConfig struct {
	Path string `yaml:"path"`
}
