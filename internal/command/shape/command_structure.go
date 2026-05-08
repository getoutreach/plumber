// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements structure command runner
package shape

import (
	"errors"
	"fmt"
	"path"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"github.com/urfave/cli/v2"
)

func RunStructure(c *cli.Context, ctx *contract.ShapingContext, cfg *Config) error {
	if cfg.StructureDefinitions == nil || len(cfg.StructureDefinitions.Structures) == 0 {
		return errors.New("structure definitions are required for RunStructure")
	}
	for _, s := range cfg.StructureDefinitions.Structures {
		if err := runSingleStructure(ctx, cfg, s); err != nil {
			return fmt.Errorf("processing structure %q: %w", s.Name, err)
		}
	}
	return nil
}

func runSingleStructure(ctx *contract.ShapingContext, cfg *Config, s config.PlumberStructureConfig) error {
	for _, pathConfig := range s.Paths {
		var (
			p     = pathConfig.Path
			scope = render.Scope{
				"File": render.Scope{
					"PackageDescription": p.PackageDescription,
					"Description":        p.Description,
					"Documentation":      p.Documentation,
				},
			}
		)
		if p.Required {
			module, err := ctx.DeriveModulePath(path.Join(ctx.Module.Dir, s.Path, p.Path))
			if err != nil {
				return err
			}
			output := path.Join(ctx.Module.Dir, s.Path, p.Path, path.Base(p.Path)+".go")

			rc := render.NewRenderContext(
				render.NewModuleRegister(),
				&model.Package{
					Name: path.Base(p.Path),
					Path: module,
				},
				output,
			)

			opts, err := ctx.TemplateLoader.Load(p.Template, p.Templates...)
			if err != nil {
				return err
			}

			rc.WithRenderOptions(render.WithBaseTemplates())

			rc.WithRenderOptions(opts...)

			tpl := p.Template

			if tpl == "" {
				tpl = "plumber/empty"
			}

			o, err := render.File(rc, tpl, scope, output)
			if err != nil {
				return fmt.Errorf("rendering structure file for path %q: %w", p.Path, err)
			}

			err = restoreOutputs(ctx, []*ManagerOutput{
				{
					Output:  o,
					Content: o.Content,
				},
			})
			if err != nil {
				return fmt.Errorf("restoring outputs for path %q: %w", p.Path, err)
			}
		}
	}
	return nil
}
