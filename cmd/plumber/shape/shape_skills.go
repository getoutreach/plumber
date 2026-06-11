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

// externalSkillSources converts the skill directories resolved during git
// checkout (stashed on shape.Config by checkoutAndMergeIncludes) into the
// ExternalSource form expected by the skills package.
func externalSkillSources(cfg *shape.Config) []skills.ExternalSource {
	if len(cfg.ExternalSkills) == 0 {
		return nil
	}
	out := make([]skills.ExternalSource, 0, len(cfg.ExternalSkills))
	for _, r := range cfg.ExternalSkills {
		origin := ""
		if r.Git != nil {
			origin = r.Git.Repository
		}
		out = append(out, skills.ExternalSource{Dir: r.Path, Origin: origin})
	}
	return out
}

// RunSkillsInstall installs embedded skills (plus any external skills declared
// via git source `skills` entries) into the requested coding-agent platform's
// filesystem layout. The first positional argument is the platform
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
		External:        externalSkillSources(shapeConfig),
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
