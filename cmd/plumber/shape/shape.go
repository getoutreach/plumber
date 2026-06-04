// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the shape CLI command entry point, parsing config and delegating to the internal shape runner.

// Package shape provides the shape subcommand for generating and transforming Go type definitions via plumber annotations.
package shape

import (
	"fmt"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/urfave/cli/v2"
)

// Run executes the shape command
func Run(c *cli.Context, ctx *contract.ShapingContext, shapeConfig *shape.Config) error {
	targets, err := shape.ParseFileTargets(c.Args().Slice())
	if err != nil {
		return fmt.Errorf("invalid file target: %w", err)
	}
	if err := shape.Run(ctx, shapeConfig, targets); err != nil {
		return fmt.Errorf("failed to run shape command: %w", err)
	}

	return nil
}

// Run executes the shape command
func RunStructure(c *cli.Context, ctx *contract.ShapingContext, shapeConfig *shape.Config) error {
	return shape.RunStructure(c, ctx, shapeConfig)
}

func RunTarget(c *cli.Context, ctx *contract.ShapingContext, shapeConfig *shape.Config) error {
	// Parse single-type targeted mode flags
	if err := parseTargetFlags(c, shapeConfig); err != nil {
		return err
	}

	targets, err := shape.ParseFileTargets(c.Args().Slice())
	if err != nil {
		return fmt.Errorf("invalid file target: %w", err)
	}

	if err := shape.RunTarget(ctx, shapeConfig, targets); err != nil {
		return fmt.Errorf("failed to run shape command: %w", err)
	}
	return nil
}
