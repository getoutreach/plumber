// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the GeneratorManager and InplaceManager for orchestrating shape transformations and rendering outputs.

package shape

import (
	"fmt"
	"path"

	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/templates"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/internal/render/view"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

func buildContext(cfg *ShapeConfig, modules *render.ModuleRegister, pkg *model.Package, output string) render.Context {
	context := render.Context{
		PkgPath: pkg.Path,
		Modules: modules,
		Wrapper: NewTypeWrapper(cfg),
		Output:  output,
		Package: pkg,
	}
	return context
}

func transformationContext(context render.Context, cfg *ShapeConfig, t Transformation) (render.Context, error) {
	names := t.Transformer.GetAnnotations().FindAll(contract.OptionTemplate).FlatArgs()
	opts, err := templates.Load(cfg.Sources, &cfg.Templates, cfg.CacheDir, names, render.EmbededTemplates)
	if err != nil {
		return context, err
	}
	context.RenderOptions = append(context.RenderOptions, opts...)

	return context, nil
}

type GeneratorManager struct {
	output  string
	pkgPath string
	cfg     *ShapeConfig
}

func NewGeneratorManager(cfg *ShapeConfig, pkgPath, output string) *GeneratorManager {
	return &GeneratorManager{
		output:  output,
		pkgPath: pkgPath,
		cfg:     cfg,
	}
}

func managerRender(cfg *ShapeConfig, pkgs []*model.Package, pkgPath string, opener gen.MemoryFileOpener, transformations []Transformation, scope map[string]any, output string) ([]*render.Output, error) {
	pkg, ok := lo.Find(pkgs, func(p *model.Package) bool {
		return p.Path == pkgPath
	})
	if !ok {
		pkg = &model.Package{
			Path: pkgPath,
		}
	}

	context := buildContext(cfg, render.NewModuleRegister(), pkg, output)

	var contents []string

	err := runTransformations(cfg, pkgs, context, gen.NewBufferFileOpener(), scope, transformations, func(content string) {
		contents = append(contents, content)
	})
	if err != nil {
		return nil, fmt.Errorf("error during rendering: %w", err)
	}

	o, err := render.Finalize(context, scope, contents, output, opener)
	if err != nil {
		return nil, fmt.Errorf("error during finalization: %w", err)
	}
	return []*render.Output{o}, nil
}

func (m *GeneratorManager) Render(pkgs []*model.Package, transformations []Transformation) ([]*render.Output, error) {
	var (
		scope = map[string]any{
			"Mode": "generated",
		}
		opener = gen.NewSystemFileOpener()
	)

	return managerRender(m.cfg, pkgs, m.pkgPath, opener, transformations, scope, m.output)
}

type InplaceManager struct {
	output  string
	pkgPath string
	cfg     *ShapeConfig
}

func NewInplaceManager(cfg *ShapeConfig, pkgPath, output string) *InplaceManager {
	return &InplaceManager{
		output:  output,
		pkgPath: pkgPath,
		cfg:     cfg,
	}
}

func (m *InplaceManager) Render(pkgs []*model.Package, transformations []Transformation) ([]*render.Output, error) {
	pkg, found := lo.Find(pkgs, func(p *model.Package) bool {
		return p.Path == m.output
	})
	var (
		scope = map[string]any{
			"Mode": "inplace",
		}
	)
	if !found {
		return nil, fmt.Errorf("package not found for output %q", m.output)
	}

	fmt.Printf("Found package %q for output %q\n", pkg.Path, m.output)

	outputs := make([]*render.Output, 0)

	modules := render.NewModuleRegister()

	for _, t := range transformations {
		var (
			opener = gen.NewBufferFileOpener()
		)
		context := buildContext(m.cfg, modules, pkg, m.output)

		var content string

		err := runTransformations(m.cfg, pkgs, context, opener, scope, []Transformation{t}, func(c string) {
			content = c
		})
		if err != nil {
			return nil, fmt.Errorf("error during rendering: %w", err)
		}

		filename := path.Join(pkg.Path, "plumber_inplace_helper.go")

		o, err := render.Finalize(context, scope, []string{content}, filename, opener)
		if err != nil {
			return nil, fmt.Errorf("error during finalization: %w", err)
		}

		f, err := decorator.Parse(o.Content)
		if err != nil {
			return nil, fmt.Errorf("failed to parse generated content for transformation %q: %w", t.Transformer.GetName(), err)
		}

		existingFile, err := Merge(pkg, f)
		if err != nil {
			return nil, fmt.Errorf("failed to merge generated content for transformation %q: %w", t.Transformer.GetName(), err)
		}

		filename = pkg.Package.Decorator.Filenames[existingFile]

		outputs = append(outputs, &render.Output{
			Filename: filename,
			Modules:  modules,
			Dst: &render.DstOutput{
				File:    existingFile,
				Package: pkg.Package,
			},
		})
	}
	//
	return outputs, nil
}

func runTransformations(
	cfg *ShapeConfig,
	pkgs []*model.Package,
	state render.Context,
	opener gen.MemoryFileOpener,
	scope map[string]any,
	transformations []Transformation, contentFunc func(string),
) (err error) {
	for _, t := range transformations {
		ignores := state.Ignores
		if ignores == nil {
			ignores = render.NewIgnores(t.Transformer.GetAnnotations().FindAll(contract.OptionIgnore).Values())
		}
		ctx := state.WithIgnores(ignores)

		ctx, err = transformationContext(ctx, cfg, t)
		if err != nil {
			return fmt.Errorf("error building transformation context for transformer %q: %w", t.Transformer.GetName(), err)
		}

		fmt.Printf("  > Transformer[%s], Line: %d\n",
			t.Transformer.GetName(),
			t.Node.GetNode().GetPosition().Line,
		)

		scope["Subject"] = view.Annotable{
			Annotations: t.Transformer.GetAnnotations(),
		}

		if err := inflateCustomScope(t.Transformer, pkgs, scope); err != nil {
			return err
		}

		content, err := t.Transformer.Render(ctx, t.Node.(*model.Type), scope, t.Transformer.Output(), opener)
		if err != nil {
			fmt.Println("Error during rendering:", err)
		}
		contentFunc(string(content))
	}
	return nil
}

// inflateCustomScope resolves all plumber:scope annotations on the transformer
// and populates scope["Custom"] with the resolved *model.Type values keyed by name.
func inflateCustomScope(transformer Transformer, pkgs []*model.Package, scope map[string]any) error {
	scopeAnnotations := transformer.GetAnnotations().FindAll(contract.OptionScope)
	if len(scopeAnnotations) == 0 {
		return nil
	}
	custom := make(map[string]any)
	for _, sa := range scopeAnnotations {
		if len(sa.Args) == 0 {
			return fmt.Errorf("plumber:scope annotation requires a name argument")
		}
		name := sa.Args[0]
		fqnStr, ok := sa.NamedArgs["type"]
		if !ok {
			return fmt.Errorf("plumber:scope annotation %q requires a type= named argument", name)
		}
		fqn, err := astx.ParseFQN(fqnStr)
		if err != nil {
			return fmt.Errorf("failed to parse FQN %q for plumber:scope %q: %w", fqnStr, name, err)
		}
		resolved := model.Packages(pkgs).TypeByFQN(fqn)
		if resolved == nil {
			return fmt.Errorf("type %q not found in packages for plumber:scope %q", fqnStr, name)
		}
		custom[name] = resolved
	}
	scope["Custom"] = custom
	return nil
}
