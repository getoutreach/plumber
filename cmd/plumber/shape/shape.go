// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the shape CLI command entry point, parsing config and delegating to the internal shape runner.

// Package shape provides the shape subcommand for generating and transforming Go type definitions via plumber annotations.
package shape

import (
	"fmt"
	"path/filepath"
	"runtime/debug"
	"strings"

	configs "github.com/getoutreach/plumber/internal/command/config"
	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/urfave/cli/v2"
)

// Run executes the shape command
func Run(c *cli.Context, shapeConfig *shape.Config) error {
	args := c.Args().Slice()
	err := shape.Run(shapeConfig, args)
	if err != nil {
		return fmt.Errorf("failed to run shape command: %w", err)
	}

	return nil
}

// Run executes the shape command
func RunStructure(c *cli.Context, shapeConfig *shape.Config) error {
	return nil
}

func RunTarget(c *cli.Context, shapeConfig *shape.Config) error {
	// Parse single-type targeted mode flags
	if err := parseTargetFlags(c, shapeConfig); err != nil {
		return err
	}

	args := c.Args().Slice()

	err := shape.Run(shapeConfig, args)
	if err != nil {
		return fmt.Errorf("failed to run shape command: %w", err)
	}
	return nil
}

func RunCommand(name string, run func(*cli.Context, *shape.Config) error) func(c *cli.Context) error {
	return func(c *cli.Context) error {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Shape command panicked: %v\n", r)
				debug.PrintStack()
			}
		}()

		configPath := c.String("config")

		shapeConfig := shape.Config{}

		if configPath != "" {
			// Resolve absolute path for config file
			absConfigPath, err := filepath.Abs(configPath)
			if err != nil {
				return fmt.Errorf("failed to resolve config path: %w", err)
			}

			// Parse and merge configuration (includes resolved automatically)
			cfg, err := configs.Load(absConfigPath)
			if err != nil {
				return fmt.Errorf("failed to parse config: %w", err)
			}

			shapeConfig = cfg.Shape
		}

		shapeConfig.Interactive = c.Bool("interactive")

		return run(c, &shapeConfig)
	}
}

// parseTargetFlags reads --type, --macro, --macro-arg, --macro-named-arg flags
// and populates shapeConfig.Target when single-type mode is requested.
func parseTargetFlags(c *cli.Context, shapeConfig *shape.Config) error {
	typeFQN := c.String("type")
	macro := c.String("macro")
	macroArgs := c.StringSlice("macro-arg")
	macroNamedArgs := c.StringSlice("macro-named-arg")

	// Parse named args from key=value format
	namedArgs := make(map[string]string, len(macroNamedArgs))
	for _, kv := range macroNamedArgs {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("--macro-named-arg must be key=value, got %q", kv)
		}
		namedArgs[parts[0]] = parts[1]
	}

	shapeConfig.Target = &config.TargetConfig{
		TypeFQN:   typeFQN,
		Macro:     macro,
		Args:      macroArgs,
		NamedArgs: namedArgs,
	}

	return nil
}
