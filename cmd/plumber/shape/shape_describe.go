// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Contains the implementation of the describe subcommand for shape.
package shape

import (
	"fmt"
	"os"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/describe"
	"github.com/getoutreach/plumber/internal/command/shape/expand"
	shaperender "github.com/getoutreach/plumber/internal/command/shape/render"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/urfave/cli/v2"
)

// RunDescribe outputs a structured description of all registered macros, options, and handlers
// in the requested format (md, json, yaml).
func RunDescribe(c *cli.Context, ctx *contract.ShapingContext, shapeConfig *shape.Config) error {
	format := c.String("format")
	formatter, err := describe.Format(format)
	if err != nil {
		return err
	}

	desc := describe.Build(shapeConfig, ctx.StructurePathResolver)
	out, err := formatter.Format(desc)
	if err != nil {
		return fmt.Errorf("failed to format description: %w", err)
	}

	_, err = os.Stdout.Write(out)
	return err
}

// RunDescribeFunctions outputs a structured description of all registered template functions
// grouped by category in the requested format (md, json, yaml).
func RunDescribeFunctions(c *cli.Context, _ *contract.ShapingContext, _ *shape.Config) error {
	format := c.String("format")
	formatter, err := describe.FunctionsFormat(format)
	if err != nil {
		return err
	}

	out, err := formatter.FormatFunctions(buildFunctionsDescription())
	if err != nil {
		return fmt.Errorf("failed to format functions description: %w", err)
	}

	_, err = os.Stdout.Write(out)
	return err
}

// RunDescribeStructures outputs a structured description of every registered
// structure and its named paths in the requested format (md, json, yaml).
func RunDescribeStructures(c *cli.Context, ctx *contract.ShapingContext, shapeConfig *shape.Config) error {
	format := c.String("format")
	formatter, err := describe.StructuresFormat(format)
	if err != nil {
		return err
	}

	structures, err := describe.BuildStructures(shapeConfig, ctx.StructurePathResolver, ctx.RepoModule, ctx.Module)
	if err != nil {
		return fmt.Errorf("failed to build structures description: %w", err)
	}

	out, err := formatter.FormatStructures(structures)
	if err != nil {
		return fmt.Errorf("failed to format structures description: %w", err)
	}

	_, err = os.Stdout.Write(out)
	return err
}

// buildFunctionsDescription gathers the same template-function descriptions
// emitted by the describe functions subcommand so that skill templates can
// reference them through the describeFunctions helper.
func buildFunctionsDescription() describe.FunctionsDescription {
	expandDesc, _ := expand.FunctionsDescription()
	shapeRenderDesc, _ := shaperender.FunctionsDescription()
	renderDesc, _ := render.FunctionsDescription()
	genericDesc, _ := render.GenericFunctionsDescription()

	return describe.BuildFunctions([]describe.FunctionSectionInput{
		{
			Title:       "Annotation Value Expansion",
			Description: "Template functions available during annotation value expansion.",
			Sources:     []contract.FunctionDescriptions{expandDesc},
		},
		{
			Title:       "Shape Template Evaluation",
			Description: "Template functions available during shape template rendering.",
			Sources:     []contract.FunctionDescriptions{shapeRenderDesc, renderDesc, genericDesc},
		},
	})
}
