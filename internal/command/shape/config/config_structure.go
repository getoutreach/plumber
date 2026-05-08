// Copyright 2026 Outreach Corporation. All Rights Reserved.
// Description: This file defines the structures configuration for the shape command.
package config

import "fmt"

// StructureConfig represents the overall configuration for the shape command, including included structures and additional structure definitions.
type StructureConfig struct {
	Include    []string                    `yaml:"include,omitempty"`
	Additional []StructureDefinitionConfig `yaml:"additional,omitempty"`
}

// StructureDefinitions holds a list of structure definitions for the shape command.
type StructureDefinitions struct {
	Structures []PlumberStructureConfig
}

// ResolveStructureDefinitions builds a StructureDefinitions by selecting structures
// whose names appear in cfg.Include from the available pool, then merging any
// cfg.Additional definitions on top (additional has priority: matching names are
// merged, new names are appended).
func ResolveStructureDefinitions(cfg StructureConfig, available []*StructureDefinitionConfig) (*StructureDefinitions, error) {
	// 1. Collect included structures by name.
	included := make([]PlumberStructureConfig, 0, len(cfg.Include))
	for _, name := range cfg.Include {
		found := false
		for _, def := range available {
			if def.Structure.Name == name {
				// Deep-copy paths slice so merges don't mutate the original config.
				s := def.Structure
				pathsCopy := make([]StructurePathConfig, len(s.Paths))
				copy(pathsCopy, s.Paths)
				s.Paths = pathsCopy
				included = append(included, s)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("structure %q not found in available definitions", name)
		}
	}

	// 2. Merge additional definitions.
	for _, add := range cfg.Additional {
		merged := false
		for i := range included {
			if included[i].Name == add.Structure.Name {
				included[i].MergeFrom(add.Structure)
				merged = true
				break
			}
		}
		if !merged {
			included = append(included, add.Structure)
		}
	}

	if len(included) == 0 {
		return nil, nil
	}

	return &StructureDefinitions{Structures: included}, nil
}

// MergeFrom merges another PlumberStructureConfig into this one.
// Fields from other override non-empty values; paths from other take
// precedence when names conflict, otherwise they are appended.
func (c *PlumberStructureConfig) MergeFrom(other PlumberStructureConfig) {
	if other.Description != "" {
		c.Description = other.Description
	}
	if other.Path != "" {
		c.Path = other.Path
	}

	for _, op := range other.Paths {
		found := false
		for i, ep := range c.Paths {
			if ep.Path.Name == op.Path.Name {
				c.Paths[i] = op // additional takes priority
				found = true
				break
			}
		}
		if !found {
			c.Paths = append(c.Paths, op)
		}
	}
}

// StructureDefinitionConfig represents the configuration for a structure in the shape command,
type StructureDefinitionConfig struct {
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
	Name               string   `yaml:"name"`
	Description        string   `yaml:"description,omitempty"`
	PackageDescription string   `yaml:"package_description,omitempty"`
	Path               string   `yaml:"path,omitempty"`
	Documentation      string   `yaml:"documentation,omitempty"`
	Template           string   `yaml:"template,omitempty"`
	Templates          []string `yaml:"templates,omitempty"`
	Required           bool     `yaml:"required,omitempty"`
}
