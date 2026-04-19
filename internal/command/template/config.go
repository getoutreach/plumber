// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Shared template configuration types used by all plumber commands for loading templates
// from local directories, git repositories, or inline content.

package template

// TemplatesFileConfig represents the root-level template configuration in a plumber config file.
// It holds both source definitions (local/git) and inline content templates.
type TemplatesFileConfig struct {
	Sources []SourceConfig  `yaml:"sources,omitempty"`
	Content []ContentConfig `yaml:"content,omitempty"`
}

// TemplatesContentConfig represents the root-level template configuration in a plumber config file.
// It holds only inline content templates.
type TemplatesContentConfig struct {
	Content []ContentConfig `yaml:"content,omitempty"`
}

// SourceConfig represents a source of templates, which can be either local or from a Git repository.
type SourceConfig struct {
	Local *LocalSourceConfig `yaml:"local,omitempty"`
	Git   *GitSourceConfig   `yaml:"git,omitempty"`
}

// LocalSourceConfig represents a local source of templates, specifying the path to the templates
// and the template file list.
type LocalSourceConfig struct {
	Path      string    `yaml:"path"`
	Templates []FileRef `yaml:"templates,omitempty"`
}

// GitSourceConfig represents a Git source of templates, specifying the repository, reference,
// and templates to use. It also supports includes for loading additional configuration files
// from the Git repository.
type GitSourceConfig struct {
	Repository string             `yaml:"repository"`
	Ref        string             `yaml:"ref,omitempty"`
	Includes   []GitIncludeConfig `yaml:"includes,omitempty"`
	Templates  []FileRef          `yaml:"templates,omitempty"`
}

// GitIncludeConfig represents an include path for loading additional configuration files
// from within a Git repository source, supporting glob patterns relative to the repository root.
type GitIncludeConfig struct {
	Path string `yaml:"path"`
}

// FileRef represents a single template file reference,
// specifying the name of the template and an optional path to the template file.
type FileRef struct {
	Name string `yaml:"name"`
	Path string `yaml:"path,omitempty"`
}

// ContentConfig represents a template configuration that includes the content of the template
// directly, allowing for inline template definitions without needing a separate file.
type ContentConfig struct {
	Name    string `yaml:"name"`
	Content string `yaml:"content"`
}

// Merge appends sources and content from another TemplatesFileConfig into this one.
func (c *TemplatesFileConfig) Merge(other *TemplatesFileConfig) {
	c.Sources = append(c.Sources, other.Sources...)
	c.Content = append(c.Content, other.Content...)
}
