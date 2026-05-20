// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the shape CLI command entry point, parsing config and delegating to the internal shape runner.

// Package shape provides the shape subcommand for generating and transforming Go type definitions via plumber annotations.
package shape

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"

	"github.com/getoutreach/plumber/internal/command"
	configs "github.com/getoutreach/plumber/internal/command/config"
	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/defaults"
	"github.com/getoutreach/plumber/internal/command/shape/describe"
	"github.com/getoutreach/plumber/internal/command/shape/expand"
	"github.com/getoutreach/plumber/internal/command/shape/handler"
	shaperender "github.com/getoutreach/plumber/internal/command/shape/render"
	"github.com/getoutreach/plumber/internal/command/shape/report/term"
	"github.com/getoutreach/plumber/internal/command/shape/report/tui"
	"github.com/getoutreach/plumber/internal/command/shape/structure"
	"github.com/getoutreach/plumber/internal/command/template"
	"github.com/urfave/cli/v2"
	"golang.org/x/mod/modfile"
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

// RunDescribe outputs a structured description of all registered macros, options, and handlers
// in the requested format (md, json, yaml).
func RunDescribe(c *cli.Context, _ *contract.ShapingContext, shapeConfig *shape.Config) error {
	format := c.String("format")
	formatter, err := describe.Format(format)
	if err != nil {
		return err
	}

	desc := describe.Build(shapeConfig)
	out, err := formatter.Format(desc)
	if err != nil {
		return fmt.Errorf("failed to format description: %w", err)
	}

	_, err = os.Stdout.Write(out)
	return err
}

func RunCommand(name string, run func(*cli.Context, *contract.ShapingContext, *shape.Config) error) func(c *cli.Context) error {
	return func(c *cli.Context) error {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Shape command panicked: %v\n", r)
				debug.PrintStack()
			}
		}()

		configPath := c.String("config")

		// Start with embedded defaults so the shape command always has
		// built-in annotation option definitions available.
		defaultCfg, err := defaults.Load()
		if err != nil {
			return fmt.Errorf("failed to load embedded defaults: %w", err)
		}
		shapeConfig := *defaultCfg

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

			shapeConfig.MergeShape(&cfg.Shape, true)
		}

		shapeConfig.Interactive = c.Bool("interactive")

		if err := checkoutAndMergeIncludes(&shapeConfig); err != nil {
			return fmt.Errorf("failed to checkout and merge includes: %w", err)
		}

		shapeConfig.Handlers = expand.Handlers(shapeConfig.Handlers)

		ctx, wait, err := prepareContext(&shapeConfig)
		if err != nil {
			return fmt.Errorf("failed to prepare context: %w", err)
		}
		defer wait()

		return run(c, ctx, &shapeConfig)
	}
}

func prepareContext(cfg *shape.Config) (shapingContext *contract.ShapingContext, cleanup func(), err error) {
	templateCache := template.NewTemplateCache(cfg.Sources, cfg.Templates.Content, cfg.CacheDir, shaperender.EmbededTemplates)
	reporter, wait := newReporter(cfg.Interactive)

	baseDir := "./"
	repoModule, err := loadRepoModuleInfo()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load repo module info: %w", err)
	}
	module, err := loadModuleInfo(repoModule, baseDir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load module info: %w", err)
	}

	structureResolver, err := structure.NewResolver(cfg.StructureDefinitions, repoModule, module)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create structure resolver: %w", err)
	}

	ctx := contract.NewShapingContext(
		context.Background(),
		reporter,
		templateCache,
		structureResolver,
	)
	ctx.BaseDir = baseDir
	ctx.RepoModule = repoModule
	ctx.Module = module
	ctx.Notifications = handler.NewRegistry(cfg.Handlers)
	return ctx, wait, err
}

// newReporter creates a reporter based on the interactive flag.
// It returns the reporter and a wait function that should be deferred
// to ensure proper cleanup (for the TUI reporter, this waits for the
// bubbletea program to finish).
func newReporter(interactive bool) (reporter contract.Reporter, cleanup func()) {
	if interactive {
		r := tui.NewReporter()
		return r, r.Wait
	}
	return term.NewTerminalReporter(), func() {}
}

// checkoutAndMergeIncludes checks out template sources from git and merges
// any config files found via git source includes into the shape config.
// It stamps Git provenance on macros and options loaded from git repos.
func checkoutAndMergeIncludes(cfg *shape.Config) error {
	results, err := template.Checkout(cfg.Sources, cfg.CacheDir)
	if err != nil {
		return fmt.Errorf("failed to checkout templates: %w", err)
	}

	for _, r := range results {
		inc, err := command.ParseConfig[shape.FileConfig](r.Path)
		if err != nil {
			return fmt.Errorf("failed to parse git include config %q: %w", r.Path, err)
		}
		stampGitProvenance(&inc.Shape, r.Git)
		cfg.MergeShape(&inc.Shape, false)
	}

	defs, err := config.ResolveStructureDefinitions(cfg.StructureConfig, cfg.Structures)
	if err != nil {
		return fmt.Errorf("resolving structure definitions: %w", err)
	}
	cfg.StructureDefinitions = defs

	return nil
}

// stampGitProvenance sets the Git provenance field on all macros and options in the shape config.
func stampGitProvenance(cfg *shape.Config, git *template.GitSourceConfig) {
	for i := range cfg.Macros {
		if cfg.Macros[i].PlumberMacro != nil {
			cfg.Macros[i].PlumberMacro.Git = git
		}
	}
	for i := range cfg.Options {
		cfg.Options[i].Git = git
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

// loadRepoModuleInfo locates the nearest go.mod by walking up from the current
// working directory and returns a ModuleInfo populated with the module path and
// the directory containing go.mod.
func loadRepoModuleInfo() (contract.ModuleInfo, error) {
	dir, err := os.Getwd()
	if err != nil {
		return contract.ModuleInfo{}, fmt.Errorf("failed to get working directory: %w", err)
	}

	// Walk up the directory tree to find the nearest go.mod.
	for {
		goModPath := filepath.Join(dir, "go.mod")
		data, err := os.ReadFile(goModPath)
		if err == nil {
			f, err := modfile.Parse(goModPath, data, nil)
			if err != nil {
				return contract.ModuleInfo{}, fmt.Errorf("failed to parse %s: %w", goModPath, err)
			}
			modulePath := f.Module.Mod.Path
			// Name is the last segment of the module path (e.g. "plumber" from
			// "github.com/getoutreach/plumber").
			name := modulePath
			if idx := strings.LastIndex(modulePath, "/"); idx >= 0 {
				name = modulePath[idx+1:]
			}
			return contract.ModuleInfo{
				Name:           name,
				NormalizedName: strings.ReplaceAll(name, "-", "_"),
				Path:           modulePath,
				RelativePath:   "",
				Dir:            dir,
			}, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return contract.ModuleInfo{}, fmt.Errorf("go.mod not found in any parent directory")
		}
		dir = parent
	}
}

// loadModuleInfo derives a ModuleInfo for baseDir relative to the repository
// root module. If baseDir points to a subdirectory, the module path is extended
// with the relative path (e.g. "github.com/foo/bar" + "internal/pkg" →
// "github.com/foo/bar/internal/pkg").
func loadModuleInfo(repoModule contract.ModuleInfo, baseDir string) (contract.ModuleInfo, error) {
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return contract.ModuleInfo{}, fmt.Errorf("failed to resolve base dir: %w", err)
	}

	// Determine the repo root directory by stripping the module's relative
	// path (empty for the root) from the working directory.
	cwd, err := os.Getwd()
	if err != nil {
		return contract.ModuleInfo{}, fmt.Errorf("failed to get working directory: %w", err)
	}

	relPath, err := filepath.Rel(cwd, absBase)
	if err != nil {
		return contract.ModuleInfo{}, fmt.Errorf("failed to compute relative path: %w", err)
	}
	relPath = filepath.ToSlash(relPath)
	if relPath == "." {
		relPath = ""
	}

	modulePath := repoModule.Path
	if relPath != "" {
		modulePath = repoModule.Path + "/" + relPath
	}

	name := modulePath
	if idx := strings.LastIndex(modulePath, "/"); idx >= 0 {
		name = modulePath[idx+1:]
	}

	return contract.ModuleInfo{
		Name:           name,
		NormalizedName: strings.ReplaceAll(name, "-", "_"),
		Path:           modulePath,
		RelativePath:   relPath,
		Dir:            absBase,
	}, nil
}
