// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the shape command runner, orchestrating annotation discovery,
// transformer building, and output restoration.

// Package shape implements the internal logic for the plumber shape command, transforming annotated
// Go types into generated or inplace output files.
package shape

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/dave/dst/decorator/resolver/gopackages"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/astx/inspect"
	"github.com/getoutreach/plumber/internal/command"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/expand"
	shaperender "github.com/getoutreach/plumber/internal/command/shape/render"
	"github.com/getoutreach/plumber/internal/command/shape/report/term"
	"github.com/getoutreach/plumber/internal/command/template"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// Run is the main entry point for the shape command, orchestrating the entire transformation process.
func Run(cfg *Config, args []string) error {
	if err := checkoutAndMergeIncludes(cfg); err != nil {
		return err
	}

	templateCache := template.NewTemplateCache(cfg.Sources, cfg.Templates.Content, cfg.CacheDir, shaperender.EmbededTemplates)

	reporter := term.NewTerminalReporter()

	ctx := &contract.ShapingContext{
		Context:        context.Background(),
		Reporter:       reporter,
		TemplateLoader: templateCache,
	}

	filenames, err := inspect.ScanFiles("./", args)
	if err != nil {
		return fmt.Errorf("failed to scan files: %w", err)
	}

	pkgs, err := inspect.Inspect(filenames, "./")
	if err != nil {
		return fmt.Errorf("failed to inspect files: %w", err)
	}

	// Single-type targeted mode: process only the specified type with the named macro.
	if cfg.Target != nil {
		return runTargeted(ctx, cfg, pkgs)
	}

	// Expand macros in all annotations before walking and building transformers.
	// This allows macros to inject entry-point annotations like plumber:derive
	// that create new transformers, which mixins cannot do.
	if err := expand.Macros(pkgs, cfg.Macros); err != nil {
		return err
	}

	transformations, err := collectTransformations(cfg, pkgs)
	if err != nil {
		return err
	}

	outputs, err := renderTransformations(ctx, cfg, pkgs, transformations)
	if err != nil {
		return err
	}

	if len(outputs) > 0 {
		return restoreOutputs(ctx, outputs)
	}

	return nil
}

// runTargeted processes a single type with a named macro, bypassing the full annotation scan.
// The macro is injected as a synthetic annotation onto the type, expanded, and then the
// standard transformer building and rendering pipeline runs for that single type.
func runTargeted(ctx *contract.ShapingContext, cfg *Config, pkgs model.Packages) error {
	typ, err := resolveTargetType(cfg.Target.TypeFQN, pkgs)
	if err != nil {
		return err
	}

	// Validate macro exists in config
	macroExists := lo.ContainsBy(cfg.Macros, func(m config.MacroConfig) bool {
		return m.PlumberMacro != nil && m.PlumberMacro.Name == cfg.Target.Macro
	})
	if !macroExists {
		return fmt.Errorf("macro %q not found in config", cfg.Target.Macro)
	}

	// Inject synthetic macro annotation onto the type
	ann := model.NewAnnotation(cfg.Target.Macro, cfg.Target.Args, model.WithNamedArgs(cfg.Target.NamedArgs))
	typ.TypeNode.Annotations = append(typ.TypeNode.Annotations, ann)

	// Expand macros (only targeted type has the injected macro)
	if err := expand.Macros(pkgs, cfg.Macros); err != nil {
		return err
	}

	// Build transformers for the single type
	ts, err := buildTransformers(cfg, typ.TypeNode)
	if err != nil {
		return fmt.Errorf("failed to build transformers for %q: %w", cfg.Target.TypeFQN, err)
	}

	// Build transformations
	var transformations []Transformation
	for _, t := range ts {
		transformations = append(transformations, Transformation{
			Node:        typ,
			Transformer: t,
			Path:        buildPath(t.Mode(), typ.GetPackage().Path, typ.GetPosition().Filename, t.Output()),
		})
	}

	// Render and restore
	outputs, err := renderTransformations(ctx, cfg, pkgs, transformations)
	if err != nil {
		return err
	}
	if len(outputs) > 0 {
		return restoreOutputs(ctx, outputs)
	}
	return nil
}

// resolveTargetType resolves the target type from the given FQN string.
// If the FQN contains a quoted package path (e.g. "github.com/pkg".Type), it uses
// full FQN resolution. Otherwise, it searches all packages by unqualified type name.
func resolveTargetType(typeFQN string, pkgs model.Packages) (*model.Type, error) {
	fqn, err := astx.ParseFQN(typeFQN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse target type FQN: %w", err)
	}

	// If the FQN has a package path, use exact FQN matching
	if !fqn.IsPackageLess() {
		typ := pkgs.TypeByFQN(fqn)
		if typ == nil {
			return nil, fmt.Errorf("target type %q not found in inspected packages", typeFQN)
		}
		return typ, nil
	}

	// Package-less name: search by type name across all packages
	typ := pkgs.TypeByName(typeFQN)
	if typ == nil {
		return nil, fmt.Errorf("target type %q not found in inspected packages", typeFQN)
	}
	return typ, nil
}

// checkoutAndMergeIncludes checks out template sources from git and merges
// any config files found via git source includes into the shape config.
func checkoutAndMergeIncludes(cfg *Config) error {
	includePaths, err := template.Checkout(cfg.Sources, cfg.CacheDir)
	if err != nil {
		return fmt.Errorf("failed to checkout templates: %w", err)
	}

	for _, p := range includePaths {
		inc, err := command.ParseConfig[FileConfig](p)
		if err != nil {
			return fmt.Errorf("failed to parse git include config %q: %w", p, err)
		}
		cfg.MergeShape(&inc.Shape)
	}
	return nil
}

// collectTransformations walks all annotated nodes and package-level comments
// to build the full list of transformations to execute.
func collectTransformations(cfg *Config, pkgs model.Packages) ([]Transformation, error) {
	var transformingNodes []model.Node

	err := inspect.Walk(pkgs, inspect.WithAnnotations(
		inspect.WithAnnotationName("plumber:shape", "plumber:derive"),
		func(node model.Node) error {
			transformingNodes = append(transformingNodes, node)
			return nil
		}))
	if err != nil {
		return nil, fmt.Errorf("failed to walk packages: %w", err)
	}

	var transformations []Transformation

	appendTransformer := func(node model.Node, ts []Transformer) {
		for _, t := range ts {
			transformations = append(transformations, Transformation{
				Node:        node,
				Transformer: t,
				Path:        buildPath(t.Mode(), node.GetPackage().Path, node.GetPosition().Filename, t.Output()),
			})
		}
	}

	for _, node := range transformingNodes {
		ts, err := buildTransformers(cfg, node)
		if err != nil {
			return nil, fmt.Errorf("failed to build transformers for node %q: %w", node.GetPosition(), err)
		}
		appendTransformer(node, ts)
	}

	// for _, t := range transformations {

	// }

	// Process package-level comments for transformations.
	for _, pkg := range pkgs {
		for _, comment := range pkg.Comments {
			m := comment.Annotations.Find(contract.OptionContext)
			if m == nil {
				continue
			}
			fqn, err := astx.ParseFQN(m.Value())
			if err != nil {
				return nil, fmt.Errorf("failed to parse model FQN %q: %w", m.Value(), err)
			}
			t := pkgs.TypeByFQN(fqn)
			if t == nil {
				return nil, fmt.Errorf("model type %q not found in packages", fqn)
			}
			ts, err := buildTransformers(cfg, comment.FilterAnnotations(func(a model.Annotation) bool {
				return a.Name != contract.OptionContext
			}))
			if err != nil {
				return nil, fmt.Errorf("failed to build transformers for node %q: %w", t.GetPosition(), err)
			}
			appendTransformer(t, ts)
		}
	}

	return transformations, nil
}

// renderTransformations groups transformations by mode and output filename,
// renders each group via the appropriate manager, and appends query outputs.
func renderTransformations(
	ctx *contract.ShapingContext,
	cfg *Config,
	pkgs []*model.Package,
	transformations []Transformation,
) ([]*ManagerOutput, error) {
	byMode := lo.GroupBy(transformations, func(t Transformation) string {
		return t.Transformer.Mode()
	})

	var outputs []*ManagerOutput

	for mode, transformations := range byMode {
		byOutput := lo.GroupBy(transformations, func(t Transformation) string {
			return t.Path.Filename
		})

		for filename, transformations := range byOutput {
			manager := buildModeManager(cfg, mode, transformations[0].Path.Package, filename)

			output, err := manager.Render(ctx, pkgs, transformations)
			if err != nil {
				return nil, fmt.Errorf("failed to render transformations for output %q: %w", filename, err)
			}
			if output == nil {
				continue
			}
			for _, o := range output {
				outputs = append(outputs, &ManagerOutput{
					Output:  o,
					Manager: manager,
				})
			}
		}
	}

	// Process plumber:query annotations on package-level variables.
	queryOutputs, err := processQueries(ctx, pkgs)
	if err != nil {
		return nil, fmt.Errorf("failed to process queries: %w", err)
	}
	for _, qo := range queryOutputs {
		outputs = append(outputs, &ManagerOutput{
			Output: &render.Output{
				Filename: qo.Filename,
				Dst: &render.DstOutput{
					File:    qo.File,
					Package: qo.Package.Package,
				},
			},
		})
	}

	return outputs, nil
}

func restoreOutputs(ctx *contract.ShapingContext, output []*ManagerOutput) error {
	filenames := []string{}
	overlay := make(map[string][]byte)

	// restore generated files out of rendered output
	for _, o := range output {
		// Dst files are processed in a separate loop below
		if o.Output.Dst != nil {
			continue
		}
		filenames = append(filenames, o.Output.Filename)
		overlay[o.Output.Filename] = o.Output.Content
	}

	if len(filenames) > 0 {
		parser, err := astx.NewParser(filenames, astx.WithReplacement(), astx.WithOverlay(overlay))
		if err != nil {
			return fmt.Errorf("failed to create parser for post-processing: %w", err)
		}

		for _, o := range output {
			if o.Output.Dst != nil {
				continue
			}
			content, pkg, err := parser.GetParsedFile(o.Output.Filename)
			if err != nil {
				return fmt.Errorf("failed to parse generated file %q: %w", o.Output.Filename, err)
			}
			err = restoreOutput(ctx, o, content, pkg)
			if err != nil {
				return fmt.Errorf("failed to restore generated file %q: %w", o.Output.Filename, err)
			}
		}
	}

	// restore dst files out of augment dst structures
	for _, o := range output {
		if o.Output.Dst == nil {
			continue
		}
		err := restoreOutput(ctx, o, o.Output.Dst.File, o.Output.Dst.Package)
		if err != nil {
			return fmt.Errorf("failed to restore generated dst file %q: %w", o.Output.Filename, err)
		}
	}

	return nil
}

// restoreOutput takes a ManagerOutput, the corresponding dst.File content, and the decorator.Package,
func restoreOutput(ctx *contract.ShapingContext, output *ManagerOutput, content *dst.File, pkg *decorator.Package) (outError error) {
	defer ctx.RestoredOutput(output.Output.Filename, outError)

	var buf bytes.Buffer
	r := decorator.NewRestorerWithImports(pkg.PkgPath, gopackages.WithHints("./", map[string]string{"time": "time"}))

	if err := r.Fprint(&buf, content); err != nil {
		return fmt.Errorf("failed to restore AST for file %q: %w", output.Output.Filename, err)
	}

	// Write to file
	if err := os.WriteFile(output.Output.Filename, buf.Bytes(), 0o600); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// buildPath constructs the output path information for a transformation based on its mode, package path, original filename,
// and desired output path.
func buildPath(mode, pkgPath, filename, output string) Pathinfo {
	baseDir := path.Dir(filename)

	actualFilename := path.Join(baseDir, output)

	pkg := path.Join(pkgPath, path.Dir(output))

	if mode == render.ModeInPlace {
		actualFilename = pkg
	}

	return Pathinfo{
		Filename: actualFilename,
		BaseDir:  baseDir,
		RelPath:  output,
		Package:  pkg,
	}
}

func buildModeManager(cfg *Config, mode, pkgPath, output string) Manager {
	switch mode {
	case render.ModeInPlace:
		return NewInplaceManager(cfg, pkgPath, output)
	case render.ModeGenerated:
		return NewGeneratorManager(cfg, pkgPath, output)
	}
	return nil
}

// buildTransformers constructs the list of transformers to apply to a given node based on its annotations and the provided configuration.
// node can be either comming from model.Type (targeted), model.CommentGroup or any other model.Node that carries annotations
func buildTransformers(cfg *Config, node model.Node) (transformers []Transformer, err error) {
	var (
		lastTransformer Transformer
	)

	changeTransformer := func(t Transformer) error {
		if lastTransformer != nil {
			if err := lastTransformer.Validate(node); err != nil {
				return err
			}
		}
		if t != nil {
			lastTransformer = t
		}
		return nil
	}

	transformers = []Transformer{}
	// Per-annotation template expansion: any annotation that was implied by a
	// macro (or mixin) carries an ImpliedBy reference. We expand its templated
	// args/namedArgs eagerly, before the transformer consumes it, so that
	// downstream stages (Validate, Render) observe fully-resolved values.
	//
	// node may or may not satisfy model.Node — e.g. *model.CommentGroup does
	// not — so we fall back to a nil model.Node when it does not, which the
	// expander handles gracefully (empty .Package, nil .Type).
	annotations := node.GetAnnotations()
	for _, annotation := range annotations {
		switch annotation.Name {
		case "plumber:shape":
			if err := changeTransformer(NewShaper(node.GetPosition(), annotation)); err != nil {
				return nil, err
			}
			transformers = append(transformers, lastTransformer)
		case "plumber:derive":
			if err := changeTransformer(NewDeriveTransformer(node.GetPosition(), annotation)); err != nil {
				return nil, err
			}
			transformers = append(transformers, lastTransformer)
		default:
			if lastTransformer == nil {
				return nil, fmt.Errorf("unexpected annotation %q without a transformer", annotation.Name)
			}
			if !lastTransformer.Accepts(annotation.Name) {
				return nil, fmt.Errorf("transformer %s does not accept annotation %q", lastTransformer.GetName(), annotation.Name)
			}
			lastTransformer.Add(annotation)
			if annotation.Name == contract.OptionMixin {
				if err := expand.Mixin(annotation, lastTransformer, cfg.Mixins); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := changeTransformer(nil); err != nil {
		return nil, err
	}

	return transformers, nil
}
