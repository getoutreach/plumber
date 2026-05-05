// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines configuration types for the shape command including template, mixin, wrapper, and include settings.

package shape

import (
	"github.com/getoutreach/plumber/internal/command/inspect"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/template"
)

// FileConfig represents the overall configuration for the shape command,
// including settings for templates, mixins, wrappers, and includes.
type FileConfig struct {
	Shape    Config                 `yaml:"plumber.shape"`
	Inspect  inspect.FileConfig     `yaml:"plumber.inspect"`
	Includes []config.IncludeConfig `yaml:"includes,omitempty"`
}

// Config holds specific configuration options for the shape command, such as working directory,
// cache directory, template sources, mixins, and type wrappers.
type Config struct {
	WorkingDir  string                       `yaml:"workingDir,omitempty"`
	CacheDir    string                       `yaml:"cacheDir,omitempty"`
	Sources     []*template.SourceConfig     `yaml:"sources,omitempty"`
	Templates   template.TemplatesFileConfig `yaml:"templates,omitempty"`
	Macros      []config.MacroConfig         `yaml:"macros,omitempty"`
	Mixins      []config.MixinConfig         `yaml:"mixins,omitempty"`
	Matchers    []config.MatcherConfig       `yaml:"matchers,omitempty"`
	Type        config.TypeConfig            `yaml:"type,omitempty"`
	Structures  config.StructuresConfig      `yaml:"structures,omitempty"`
	Target      *config.TargetConfig         `yaml:"-"`
	Interactive bool                         `yaml:"-"`
}

func (c *FileConfig) Merge(includes ...*FileConfig) {
	for _, include := range includes {
		c.Shape.MergeShape(&include.Shape)
	}
}

// MergeShape merges another Config into this one, appending sources, templates, mixins, matchers, and wrappers.
func (c *Config) MergeShape(other *Config) {
	c.Sources = append(c.Sources, other.Sources...)
	c.Templates.Content = append(c.Templates.Content, other.Templates.Content...)
	c.Macros = append(c.Macros, other.Macros...)
	c.Mixins = append(c.Mixins, other.Mixins...)
	c.Matchers = append(c.Matchers, other.Matchers...)
	c.Type.Wrappers = append(c.Type.Wrappers, other.Type.Wrappers...)
}
