// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides embedded default annotation option definitions for the shape command.

// Package defaults embeds and exposes the built-in annotation option definitions
// that are always available to the shape command regardless of user configuration.
package defaults

import (
	"embed"
	"fmt"

	"github.com/getoutreach/plumber/internal/command"
	"github.com/getoutreach/plumber/internal/command/shape"
)

//go:embed defaults.yaml
var defaultsFS embed.FS

// Load parses the embedded defaults.yaml and returns the default shape Config
// containing all built-in annotation option definitions.
func Load() (*shape.Config, error) {
	data, err := defaultsFS.ReadFile("defaults.yaml")
	if err != nil {
		return nil, fmt.Errorf("reading embedded defaults.yaml: %w", err)
	}

	cfg, err := command.ParseConfigBytes[shape.FileConfig](data)
	if err != nil {
		return nil, fmt.Errorf("parsing embedded defaults.yaml: %w", err)
	}

	return &cfg.Shape, nil
}
