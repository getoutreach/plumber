// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Configuration models for plumber discovery
// Managed: true

package discovery

import (
	"github.com/getoutreach/plumber/internal/command/template"
)

// Config represents the root configuration for plumber discovery
type Config struct {
	Applications []Application `yaml:"applications"`
	Templates    Templates     `yaml:"templates,omitempty"`
}

// Templates configures which templates to use when rendering discovery files.
// Global templates are applied to all file renders. Container and Application
// templates are additive per-file overrides. Each entry can be either inline
// content (has Content field) or a name reference resolved from the root-level
// plumber.templates registry.
type Templates struct {
	Global      []template.ContentConfig `yaml:"global,omitempty"`
	Container   []template.ContentConfig `yaml:"container,omitempty"`
	Application []template.ContentConfig `yaml:"application,omitempty"`
}

// Application represents a single application module
type Application struct {
	Name        string                 `yaml:"name"`
	Module      string                 `yaml:"module,omitempty"`
	Config      string                 `yaml:"config,omitempty"`
	Application *ApplicationPathConfig `yaml:"application,omitempty"`
	Containers  []Container            `yaml:"containers"`
}

// ApplicationPathConfig specifies the path to the root application container file.
type ApplicationPathConfig struct {
	Path string `yaml:"path"`
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

	// AdditionalSources holds source paths of other loop matches that were
	// merged into this container because they hydrated to the same container
	// name and path (e.g. multiple structure paths sharing one
	// dependency_container). Not configurable.
	AdditionalSources []SourcePathConfig `yaml:"-"`
}

// Sources returns all source paths of the container: the configured Source
// followed by any AdditionalSources merged during loop expansion.
func (c *PlumberContainerConfig) Sources() []SourcePathConfig {
	var sources []SourcePathConfig
	if c.Source != nil {
		sources = append(sources, *c.Source)
	}
	return append(sources, c.AdditionalSources...)
}

// ContainerPathConfig specifies the path to the container definition
type ContainerPathConfig struct {
	Path string `yaml:"path"`
}

// SourcePathConfig specifies the path to source files for discovery
type SourcePathConfig struct {
	Path string `yaml:"path"`
}

// ConstructorPattern represents a single constructor matching pattern
type ConstructorPattern struct {
	// Re is a regular expression pattern with optional named capture groups
	Re string `yaml:"re"`
	// Rule is an optional rule that must be satisfied for the pattern to match.
	// Supported formats:
	//   - "annotation.has:<name>" — the function must have the specified annotation in its doc comment
	Rule string `yaml:"rule,omitempty"`
}

// Matcher represents a matcher configuration
type Matcher struct {
	// Constructor patterns with optional named capture groups
	Constructors []ConstructorPattern `yaml:"constructors,omitempty"`
}

// LoopConfig defines how to iterate over paths to create multiple containers
type LoopConfig struct {
	Path    string   `yaml:"path"`
	Paths   []string `yaml:"paths,omitempty"`
	BaseDir string   `yaml:"baseDir,omitempty"`
}

// MergeDiscovery merges another Config into this one, appending applications.
func (c *Config) MergeDiscovery(other *Config) {
	c.Applications = append(c.Applications, other.Applications...)
	c.Templates.Global = append(c.Templates.Global, other.Templates.Global...)
	c.Templates.Container = append(c.Templates.Container, other.Templates.Container...)
	c.Templates.Application = append(c.Templates.Application, other.Templates.Application...)
}
