// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the inspect CLI command entry point, parsing config and delegating to the internal inspect runner.

// Package inspect provides the inspect subcommand for discovering and printing type information from Go source files.
package inspect

import (
	"fmt"
	"path/filepath"

	"github.com/getoutreach/plumber/internal/command/config"
	"github.com/getoutreach/plumber/internal/command/inspect"
	"github.com/urfave/cli/v2"
)

// Run executes the discovery command
func Run(c *cli.Context) error {
	args := c.Args().Slice()

	configPath := c.String("config")
	format := c.String("format")

	cfg := &inspect.Config{}

	if configPath != "" {
		// Resolve absolute path for config file
		absConfigPath, err := filepath.Abs(configPath)
		if err != nil {
			return fmt.Errorf("failed to resolve config path: %w", err)
		}

		// Parse the configuration
		fileCfg, err := config.Load(absConfigPath)
		if err != nil {
			return fmt.Errorf("failed to parse config: %w", err)
		}
		cfg = &fileCfg.Inspect
	}

	if format != "" {
		cfg.Format = format
	}
	if cfg.Format == "" {
		cfg.Format = "json" // default to json if not specified
	}

	err := inspect.Run(cfg, args)
	if err != nil {
		return fmt.Errorf("failed to run inspect command: %w", err)
	}

	return nil
}
