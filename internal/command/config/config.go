// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Unified configuration types for plumber CLI commands, providing a single FileConfig structure
// that encompasses discovery, shape, and inspect configurations with shared include support.

// Package config defines the unified configuration structure for all plumber CLI commands.
package config

import (
	"github.com/getoutreach/plumber/internal/command/discovery"
	"github.com/getoutreach/plumber/internal/command/inspect"
	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/template"
)

// FileConfig represents the unified configuration file for all plumber commands.
// Each command reads only the section(s) it needs; unrelated sections are ignored.
type FileConfig struct {
	Templates template.TemplatesFileConfig `yaml:"plumber.templates"`
	Discovery discovery.Config             `yaml:"plumber.discovery"`
	Shape     shape.Config                 `yaml:"plumber.shape"`
	Inspect   inspect.Config               `yaml:"plumber.inspect"`
	Includes  []IncludeConfig              `yaml:"includes,omitempty"`
}

// IncludeConfig represents a path to an additional configuration file to merge.
// Glob patterns are supported (e.g., "plumber.d/*.yaml").
type IncludeConfig struct {
	Path string `yaml:"path"`
}

// Merge merges included FileConfigs into the receiver, appending additive fields
// from each section.
func (c *FileConfig) Merge(includes ...*FileConfig) {
	for _, include := range includes {
		c.Templates.Merge(&include.Templates)
		c.Shape.MergeShape(&include.Shape)
		c.Discovery.MergeDiscovery(&include.Discovery)
	}
}
