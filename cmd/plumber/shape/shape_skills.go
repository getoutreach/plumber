// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Contains the implementation of the skills subcommand for shape.

package shape

import (
	"fmt"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/describe"
	"github.com/getoutreach/plumber/internal/command/shape/skills"
	"github.com/urfave/cli/v2"
)

// RunSkillsList lists every embedded plumber skill along with its frontmatter
// description.
func RunSkillsList(_ *cli.Context, _ *contract.ShapingContext, _ *shape.Config) error {
	infos, err := skills.ListSkills()
	if err != nil {
		return fmt.Errorf("listing skills: %w", err)
	}
	for _, info := range infos {
		if info.Description != "" {
			fmt.Printf("%s\t%s\n", info.Name, info.Description)
		} else {
			fmt.Println(info.Name)
		}
	}
	return nil
}

// RunSkillsInstall installs embedded skills into the requested coding-agent
// platform's filesystem layout. The first positional argument is the platform
// (agents|claude|copilot|autodetect); subsequent arguments restrict the
// install to specific skill names. Templates within skill markdown files are
// expanded with describe output (macros, options, handlers, functions).
func RunSkillsInstall(c *cli.Context, ctx *contract.ShapingContext, shapeConfig *shape.Config) error {
	args := c.Args().Slice()
	if len(args) == 0 {
		return fmt.Errorf("missing platform argument (expected one of: agents, claude, copilot, autodetect)")
	}
	platform, err := skills.ParsePlatform(args[0])
	if err != nil {
		return err
	}

	destRoot := c.String("dest")
	if destRoot == "" {
		destRoot = "."
	}
	platforms, err := skills.ResolvePlatforms(platform, destRoot)
	if err != nil {
		return err
	}

	structures, err := describe.BuildStructures(shapeConfig, ctx.StructurePathResolver, ctx.RepoModule, ctx.Module)
	if err != nil {
		return fmt.Errorf("failed to build structures description: %w", err)
	}

	tmplCtx := skills.TemplateContext{
		Description: describe.Build(shapeConfig, ctx.StructurePathResolver),
		Functions:   buildFunctionsDescription(),
		Structures:  structures,
	}

	results, err := skills.Install(skills.InstallOptions{
		Platforms:       platforms,
		Skills:          args[1:],
		DestRoot:        destRoot,
		Force:           c.Bool("force"),
		DryRun:          c.Bool("dry-run"),
		TemplateContext: tmplCtx,
	})
	if err != nil {
		return err
	}

	printSkillResults(results, c.Bool("dry-run"))
	return nil
}

// printSkillResults writes a concise summary of an install run to stdout.
func printSkillResults(results []skills.InstallResult, dryRun bool) {
	prefix := ""
	if dryRun {
		prefix = "[dry-run] "
	}
	for _, r := range results {
		fmt.Printf("%s%s -> %s (%s)\n", prefix, r.Skill, r.Destination, r.Platform)
		for _, f := range r.Files {
			switch {
			case f.Skipped:
				fmt.Printf("  - skipped %s (%s)\n", f.Path, f.Reason)
			default:
				fmt.Printf("  - wrote   %s\n", f.Path)
			}
		}
	}
}
