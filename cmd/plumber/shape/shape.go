// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the shape CLI command entry point, parsing config and delegating to the internal shape runner.

// Package shape provides the shape subcommand for generating and transforming Go type definitions via plumber annotations.
package shape

import (
	"fmt"
	"path/filepath"

	"github.com/getoutreach/plumber/internal/command/config"
	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/urfave/cli/v2"
)

// Run executes the discovery command
func Run(c *cli.Context) error {
	args := c.Args().Slice()

	configPath := c.String("config")

	shapeConfig := shape.Config{}

	if configPath != "" {
		// Resolve absolute path for config file
		absConfigPath, err := filepath.Abs(configPath)
		if err != nil {
			return fmt.Errorf("failed to resolve config path: %w", err)
		}

		// Parse and merge configuration (includes resolved automatically)
		cfg, err := config.Load(absConfigPath)
		if err != nil {
			return fmt.Errorf("failed to parse config: %w", err)
		}

		shapeConfig = cfg.Shape
	}

	err := shape.Run(&shapeConfig, args)
	if err != nil {
		return fmt.Errorf("failed to run shape command: %w", err)
	}

	return nil
}
