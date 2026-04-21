// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the shape CLI command entry point, parsing config and delegating to the internal shape runner.

// Package shape provides the shape subcommand for generating and transforming Go type definitions via plumber annotations.
package shape

import (
	"fmt"
	"path/filepath"
	"runtime/debug"
	"strings"

	"github.com/getoutreach/plumber/internal/command/config"
	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/urfave/cli/v2"
)

// Run executes the shape command
func Run(c *cli.Context) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Shape command panicked: %v\n", r)
			debug.PrintStack()
		}
	}()
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

	// Parse single-type targeted mode flags
	if err := parseTargetFlags(c, &shapeConfig); err != nil {
		return err
	}

	err := shape.Run(&shapeConfig, args)
	if err != nil {
		return fmt.Errorf("failed to run shape command: %w", err)
	}

	return nil
}

// parseTargetFlags reads --type, --macro, --macro-arg, --macro-named-arg flags
// and populates shapeConfig.Target when single-type mode is requested.
func parseTargetFlags(c *cli.Context, shapeConfig *shape.Config) error {
	typeFQN := c.String("type")
	macro := c.String("macro")
	macroArgs := c.StringSlice("macro-arg")
	macroNamedArgs := c.StringSlice("macro-named-arg")

	// Nothing to do if neither flag is set
	if typeFQN == "" && macro == "" {
		return nil
	}

	// Validate mutual requirements
	if typeFQN != "" && macro == "" {
		return fmt.Errorf("--macro is required when --type is specified")
	}
	if macro != "" && typeFQN == "" {
		return fmt.Errorf("--type is required when --macro is specified")
	}

	// Parse named args from key=value format
	namedArgs := make(map[string]string, len(macroNamedArgs))
	for _, kv := range macroNamedArgs {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("--macro-named-arg must be key=value, got %q", kv)
		}
		namedArgs[parts[0]] = parts[1]
	}

	shapeConfig.Target = &shape.TargetConfig{
		TypeFQN:   typeFQN,
		Macro:     macro,
		Args:      macroArgs,
		NamedArgs: namedArgs,
	}

	return nil
}
