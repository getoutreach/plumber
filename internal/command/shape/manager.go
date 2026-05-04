// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the GeneratorManager and InplaceManager for orchestrating shape transformations and rendering outputs.

package shape

import (
	"fmt"
	"path"

	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/render"
	"github.com/getoutreach/plumber/internal/command/shape/render/view"
	"github.com/getoutreach/plumber/internal/genius/gen"
	baserender "github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// buildContext constructs the rendering context for a given transformation, populating it with the package path, module register,
// type wrapper, and output path based on the provided configuration and package information.
func buildContext(cfg *Config, modules *baserender.ModuleRegister, pkg *model.Package, output string) *render.Context {
	context := &render.Context{
		ContextCloner: baserender.NewRenderContext(modules, pkg, output),
		Wrapper:       NewTypeWrapper(cfg),
	}
	return context
}

// transformationContext is a helper function that builds the rendering context for a given transformation,
// loading any necessary templates based on the transformer's annotations and the shaping context's template loader.
func transformationContext(context *render.Context, ctx *contract.ShapingContext, t *Transformation) (*render.Context, error) {
	names := t.Transformer.GetAnnotations().FindAll(contract.OptionTemplate).FlatArgs()
	opts, err := ctx.TemplateLoader.Load(names)
	if err != nil {
		return context, err
	}
	context.WithPriorityRenderOptions(opts...)

	return context, nil
}

// GeneratorManager is responsible for rendering transformations and generating new output files based on the specified output package path.
type GeneratorManager struct {
	output  string
	pkgPath string
	cfg     *Config
}

// NewGeneratorManager creates a new instance of GeneratorManager with the specified configuration, package path, and output path.
func NewGeneratorManager(cfg *Config, pkgPath, output string) *GeneratorManager {
	return &GeneratorManager{
		output:  output,
		pkgPath: pkgPath,
		cfg:     cfg,
	}
}

// managerRender is a helper function that performs the rendering of transformations for both
// GeneratorManager and InplaceManager, handling the common logic of building the context, running transformations,
// and finalizing the output.
func managerRender(
	ctx *contract.ShapingContext,
	cfg *Config,
	pkgs []*model.Package,
	pkgPath string,
	opener gen.MemoryFileOpener,
	transformations []Transformation,
	scope baserender.Scope,
	output string,
) ([]*baserender.Output, error) {
	pkg, ok := lo.Find(pkgs, func(p *model.Package) bool {
		return p.Path == pkgPath
	})
	if !ok {
		pkg = &model.Package{
			Path: pkgPath,
		}
	}
	// Defensive: ensure Dir is set so downstream consumers (e.g. Merge ->
	// findOrCreateOutputFile) can reconstruct an absolute filesystem path even when
	// the package was synthesised on-the-fly or pulled from a list that wasn't
	// previously inflated.
	pkg.EnsureDir()

	context := buildContext(cfg, baserender.NewModuleRegister(), pkg, output)

	var contents []string

	output, ok = runTransformations(ctx, cfg, pkgs, context, gen.NewBufferFileOpener(), scope, transformations, func(content string) {
		contents = append(contents, content)
	})
	if !ok {
		return nil, contract.TransformerRenderError
	}

	o, err := render.Finalize(context, scope, contents, output, opener)
	if err != nil {
		return nil, fmt.Errorf("error during finalization: %w", err)
	}
	return []*baserender.Output{o}, nil
}

// Render implements the Manager interface for GeneratorManager, orchestrating the rendering of transformations and generating
// new output files based on the specified output package path.
func (m *GeneratorManager) Render(
	ctx *contract.ShapingContext,
	pkgs []*model.Package,
	transformations []Transformation,
) ([]*baserender.Output, error) {
	var (
		scope = baserender.Scope{
			"Mode": baserender.ModeGenerated,
		}
		opener = gen.NewSystemFileOpener()
	)

	return managerRender(ctx, m.cfg, pkgs, m.pkgPath, opener, transformations, scope, m.output)
}

// InplaceManager is responsible for rendering transformations and merging the generated content into
// existing source files based on the specified output package path.
type InplaceManager struct {
	output  string
	pkgPath string
	cfg     *Config
}

// NewInplaceManager creates a new instance of InplaceManager with the specified configuration, package path, and output path.
func NewInplaceManager(cfg *Config, pkgPath, output string) *InplaceManager {
	return &InplaceManager{
		output:  output,
		pkgPath: pkgPath,
		cfg:     cfg,
	}
}

// Render implements the Manager interface for InplaceManager, orchestrating the rendering of transformations and merging
// the generated content into existing source files based on the specified output package path.
func (m *InplaceManager) Render(
	ctx *contract.ShapingContext,
	pkgs []*model.Package,
	transformations []Transformation,
) ([]*baserender.Output, error) {
	pkg, found := lo.Find(pkgs, func(p *model.Package) bool {
		return p.Path == m.output
	})
	var (
		scope = baserender.Scope{
			"Mode": baserender.ModeInPlace,
		}
	)
	if !found {
		return nil, fmt.Errorf("package not found for output %q", m.output)
	}
	// Make sure the package's filesystem directory has been resolved before any
	// downstream consumer (e.g. Merge -> findOrCreateOutputFile) needs it.
	pkg.EnsureDir()

	outputs := make([]*baserender.Output, 0)

	modules := baserender.NewModuleRegister()

	for _, t := range transformations {
		err := func(t Transformation) error {
			var (
				opener = gen.NewBufferFileOpener()
			)
			context := buildContext(m.cfg, modules, pkg, m.output)

			var content string

			_, ok := runTransformations(ctx, m.cfg, pkgs, context, opener, scope, []Transformation{t}, func(c string) {
				content = c
			})
			if !ok {
				return nil
			}

			filename := path.Join(pkg.Path, "plumber_inplace_helper.go")

			o, err := render.Finalize(context, scope, []string{content}, filename, opener)
			if err != nil {
				return fmt.Errorf("error during finalization: %w", err)
			}

			f, err := decorator.Parse(o.Content)
			if err != nil {
				return &contract.SyntaxError{
					Content: string(o.Content),
					Err:     fmt.Errorf("failed to parse generated content for transformation %q: %w", t.Transformer.GetName(), err),
				}
			}

			mergedFiles, err := Merge(pkg, f, t.Transformer.Output())
			if err != nil {
				return fmt.Errorf("failed to merge generated content for transformation %q: %w", t.Transformer.GetName(), err)
			}

			// A single transformation may touch multiple files (struct in one file,
			// method in another, missing entity creating a new file, ...). Emit one
			// output per modified file so each is restored independently.
			for _, existingFile := range mergedFiles {
				filename = pkg.Package.Decorator.Filenames[existingFile]
				ctx.TransformerOutput(t.Transformer, filename)
				outputs = append(outputs, &baserender.Output{
					Filename: filename,
					Modules:  modules,
					Dst: &baserender.DstOutput{
						File:    existingFile,
						Package: pkg.Package,
					},
				})
			}
			return nil
		}(t)
		if err != nil {
			ctx.TransformerError(t.Transformer, t.Node, err)
		}
	}
	//
	return outputs, nil
}

// Postprocess is not needed for InplaceManager since the merging is done during the Render phase,
// so we can omit it or leave it as a no-op.
func runTransformations(
	ctx *contract.ShapingContext,
	cfg *Config,
	pkgs []*model.Package,
	state *render.Context,
	opener gen.MemoryFileOpener,
	scope baserender.Scope,
	transformations []Transformation, contentFunc func(string),
) (output string, ok bool) {
	ok = true
	for _, t := range transformations {
		err := func(t Transformation) error {
			if err := inflateCustomScope(t.Transformer, pkgs, scope); err != nil {
				return err
			}

			if err := t.Transformer.Expand(t.Node, scope); err != nil {
				ctx.TransformerError(t.Transformer, t.Node, err)
				return nil
			}
			ctx.TransformerAdded(t.Transformer, t.Node)

			scope["Subject"] = view.Annotable{
				Annotations: t.Transformer.GetAnnotations(),
			}

			// Skip the entire transformation when any plumber:depends_on dependency cannot
			// be resolved in the inspected packages. This allows transformers to opt out
			// gracefully when their required collaborators are absent (e.g. an optional
			// adapter package that hasn't been generated yet).
			satisfied, err := dependsOnSatisfied(t.Transformer, pkgs)
			if err != nil {
				return fmt.Errorf("error evaluating plumber:depends_on for transformer %q: %w", t.Transformer.GetName(), err)
			}
			if !satisfied {
				ctx.TransformerSkipped(t.Transformer, t.Node, "unmet plumber:depends_on dependency")
				return nil
			}

			ignores := state.Ignores
			if ignores == nil {
				ignores = render.NewIgnores(t.Transformer.GetAnnotations().FindAll(contract.OptionIgnore).Values())
			}

			ctxPtr, err := transformationContext(state.WithIgnores(ignores), ctx, &t)
			if err != nil {
				return fmt.Errorf("error building transformation context for transformer %q: %w", t.Transformer.GetName(), err)
			}

			content, err := t.Transformer.Render(ctxPtr, t.Node.(*model.Type), scope, t.Transformer.Output(), opener)
			if err != nil {
				ctx.TransformerError(t.Transformer, t.Node, err)
			}
			contentFunc(content)
			return nil
		}(t)
		if err != nil {
			ok = false
			ctx.TransformerError(t.Transformer, t.Node, err)
			continue
		}
		// Output was expanded
		output = t.Transformer.Output()
	}
	return output, ok
}

// dependsOnSatisfied evaluates every plumber:depends_on annotation on the transformer
// and reports whether all referenced FQNs resolve to a type within the inspected
// packages. The boolean result is true when every dependency resolves (or when no
// dependency annotations are present); it is false as soon as a single dependency
// cannot be resolved. An error is returned only when an annotation is malformed
// (missing argument or invalid FQN), mirroring the behavior of inflateCustomScope.
func dependsOnSatisfied(transformer Transformer, pkgs []*model.Package) (bool, error) {
	dependsOn := transformer.GetAnnotations().FindAll(contract.OptionDependsOn)
	if len(dependsOn) == 0 {
		return true, nil
	}
	for _, da := range dependsOn {
		fqnStr := da.Value()
		if fqnStr == "" {
			return false, fmt.Errorf("plumber:depends_on annotation requires a type FQN argument")
		}
		fqn, err := astx.ParseFQN(fqnStr)
		if err != nil {
			return false, fmt.Errorf("failed to parse FQN %q for plumber:depends_on: %w", fqnStr, err)
		}
		if model.Packages(pkgs).TypeByFQN(fqn) == nil {
			return false, nil
		}
	}
	return true, nil
}

// inflateCustomScope resolves all plumber:scope annotations on the transformer
// and populates scope["Custom"] with the resolved *model.Type values keyed by name.
func inflateCustomScope(transformer Transformer, pkgs []*model.Package, scope baserender.Scope) error {
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
