// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Configuration loader that parses a YAML file, resolves includes via glob patterns, and merges them.

package config

import (
	"fmt"

	"github.com/getoutreach/plumber/internal/command"
)

// Load parses the configuration at path, resolves any includes, and returns
// the fully merged FileConfig. Shape-specific sources and content are promoted
// to the root-level Templates so all commands can access them.
func Load(path string) (*FileConfig, error) {
	cfg, err := command.ParseConfig[FileConfig](path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config %q: %w", path, err)
	}

	if len(cfg.Includes) > 0 {
		// Resolve include paths (supports glob patterns)
		var includePaths []string
		for _, inc := range cfg.Includes {
			includePaths = append(includePaths, inc.Path)
		}

		includes, err := command.ParseConfigs[FileConfig](includePaths...)
		if err != nil {
			return nil, fmt.Errorf("failed to parse included configs: %w", err)
		}

		cfg.Merge(includes...)
	}

	// Promote shape-specific sources and content to root-level Templates
	// so all commands (including discovery) can access them.
	cfg.Templates.Sources = append(cfg.Templates.Sources, cfg.Shape.Sources...)
	cfg.Templates.Content = append(cfg.Templates.Content, cfg.Shape.Templates.Content...)

	return cfg, nil
}
