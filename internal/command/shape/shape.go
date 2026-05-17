// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the shape command runner, orchestrating annotation discovery,
// transformer building, and output restoration.

// Package shape implements the internal logic for the plumber shape command, transforming annotated
// Go types into generated or inplace output files.
package shape

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/dave/dst/decorator/resolver/gopackages"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/astx/inspect"
	"github.com/getoutreach/plumber/internal/command"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/expand"
	"github.com/getoutreach/plumber/internal/command/shape/matcher"
	"github.com/getoutreach/plumber/internal/command/template"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// Run is the main entry point for the shape command, orchestrating the entire transformation process.
func Run(ctx *contract.ShapingContext, cfg *Config, targets []FileTarget) error {
	dirs := cfg.WorkingDirs
	if len(dirs) == 0 {
		dirs = []string{"./..."}
	}

	filenames, err := inspect.ScanFiles("./", dirs)
	if err != nil {
		return fmt.Errorf("failed to scan files: %w", err)
	}

	pkgs, err := inspect.Inspect(filenames, "./")
	if err != nil {
		return fmt.Errorf("failed to inspect files: %w", err)
	}

	// Expand macros in all annotations before walking and building transformers.
	// This allows macros to inject entry-point annotations like plumber:derive
	// that create new transformers, which mixins cannot do.
	if err := expand.Macros(pkgs, cfg.Macros); err != nil {
		return err
	}

	transformations, err := collectTransformations(ctx, cfg, pkgs)
	if err != nil {
		return err
	}

	// Filter transformations by file targets when provided.
	transformations, err = filterTransformations(transformations, targets)
	if err != nil {
		return err
	}

	if err := expandTransformations(ctx, cfg, pkgs, transformations); err != nil {
		return err
	}

	if err := executeTransformations(ctx, cfg, pkgs, transformations); err != nil {
		return err
	}

	// Execute handlers that were triggered by plumber:notify annotations.
	if err := executeHandlers(ctx, cfg); err != nil {
		return err
	}

	return nil
}

// RunTarget is the entry point for the shape target subcommand, which processes a single specified type with a named macro,
// bypassing the full annotation scan.
func RunTarget(ctx *contract.ShapingContext, cfg *Config, targets []FileTarget) error {
	dirs := cfg.WorkingDirs
	if len(dirs) == 0 {
		dirs = []string{"./..."}
	}

	filenames, err := inspect.ScanFiles("./", dirs)
	if err != nil {
		return fmt.Errorf("failed to scan files: %w", err)
	}

	pkgs, err := inspect.Inspect(filenames, "./")
	if err != nil {
		return fmt.Errorf("failed to inspect files: %w", err)
	}

	return runTargeted(ctx, cfg, pkgs)
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
			//Path:        buildPath(t.Mode(), t.GetPackage().Path, typ.GetPosition().Filename, t.Output()),
		})
	}

	// Render and restore using the same interleaved pipeline as Run.
	if err := executeTransformations(ctx, cfg, pkgs, transformations); err != nil {
		return err
	}

	return executeHandlers(ctx, cfg)
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
func collectTransformations(ctx *contract.ShapingContext, cfg *Config, pkgs model.Packages) ([]Transformation, error) {
	var transformingNodes []model.Node

	err := inspect.Walk(pkgs, inspect.WithAnnotations(
		inspect.WithAnnotationName(contract.TransformationDerive, contract.TransformationShape, contract.TransformationRender),
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

	if err := collectCommentTransformations(ctx, cfg, pkgs, appendTransformer); err != nil {
		return nil, fmt.Errorf("failed to collect comment transformations: %w", err)
	}

	return transformations, nil
}

// collectCommentTransformations processes package-level comments for transformations.
// It supports two modes for the plumber:context annotation:
//   - Single type: plumber:context "pkg/path".TypeName — targets a single type by FQN.
//   - Package matcher: plumber:context pkg/path matcher=<name> — targets all types in the
//     package that match the named matcher's rules.
func collectCommentTransformations(
	ctx *contract.ShapingContext,
	cfg *Config,
	pkgs model.Packages,
	appendTransformer func(node model.Node, ts []Transformer)) error {
	for _, pkg := range pkgs {
		for _, comment := range pkg.Comments {
			m := comment.Annotations.Find(contract.OptionContext)
			if m == nil {
				continue
			}

			matcherName := m.NamedArgs["matcher"]

			if matcherName != "" {
				// Package + matcher mode: match all types in the target package.
				if err := collectMatcherContextTransformations(cfg, pkgs, comment, m.Value(), matcherName, appendTransformer); err != nil {
					return err
				}
				continue
			}

			var resolveErr error
			fqn, err := astx.ParseRelativeFQN(pkg.Package.PkgPath, m.Value(), func(pkgPath, typeName string) (replacement string, ok bool) {
				resolvedPath, err := ctx.StructurePathResolver.ResolvePath(pkgPath)
				if err != nil {
					resolveErr = err
					return "", false
				}
				return resolvedPath, resolvedPath != pkgPath
			})
			if resolveErr != nil {
				return fmt.Errorf("failed to resolve package path for context annotation %q: %w", m.Value(), resolveErr)
			}

			if err != nil {
				return fmt.Errorf("failed to parse model FQN %q: %w", m.Value(), err)
			}
			t := pkgs.TypeByFQN(fqn)
			if t == nil {
				return fmt.Errorf("model type %s not found in packages", fqn)
			}

			filtered := comment.FilterAnnotations(func(a model.Annotation) bool {
				return a.Name != contract.OptionContext
			})

			ts, err := buildTransformers(cfg, filtered)
			if err != nil {
				return fmt.Errorf("failed to build transformers for node %q: %w", comment.GetPosition(), err)
			}

			if len(ts) == 0 {
				return fmt.Errorf("no transformers built for node %q with context annotation", t.GetPosition())
			}

			appendTransformer(t, ts)
		}
	}
	return nil
}

// collectMatcherContextTransformations finds all types in the given package that
// match the named matcher's rules and builds transformations for each.
func collectMatcherContextTransformations(
	cfg *Config,
	pkgs model.Packages,
	comment *model.CommentGroup,
	pkgPath string,
	matcherName string,
	appendTransformer func(node model.Node, ts []Transformer),
) error {
	if strings.HasPrefix(pkgPath, "..") {
		pkgPath = path.Clean(path.Join(comment.Package.Path, pkgPath))
	}
	targetPkg, found := lo.Find(pkgs, func(p *model.Package) bool {
		return p.Path == pkgPath
	})
	if !found {
		return fmt.Errorf("package %q not found in inspected packages", pkgPath)
	}

	m, ok := matcher.FindMatcher(cfg.Matchers, matcherName)
	if !ok {
		return fmt.Errorf("matcher %q not found in config", matcherName)
	}

	for _, t := range targetPkg.Types {
		if !matcher.MatchRules(m.Matches, &t.Spec, t) {
			continue
		}

		ts, err := buildTransformers(cfg, comment.FilterAnnotations(func(a model.Annotation) bool {
			return a.Name != contract.OptionContext
		}))
		if err != nil {
			return fmt.Errorf("failed to build transformers for matched type %q: %w", t.Name, err)
		}
		appendTransformer(t, ts)
	}

	return nil
}

// renderTransformations groups transformations by mode and output filename,
// and renders each group via the appropriate manager.
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
			return t.Transformer.Output()
		})

		for filename, transformations := range byOutput {
			pkgPath, err := ctx.DeriveModulePath(path.Dir(filename))
			if err != nil {
				return nil, fmt.Errorf("failed to derive module path for output %q: %w", filename, err)
			}
			pkg, found := lo.Find(pkgs, func(p *model.Package) bool {
				return p.Path == pkgPath
			})
			if !found {
				return nil, fmt.Errorf("package not found for output %q considered: %q", filename, pkgPath)
			}

			manager := buildModeManager(cfg, pkg, mode, filename)
			if manager == nil {
				return nil, fmt.Errorf("unsupported transformation mode: %q", mode)
			}

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
			return fmt.Errorf("failed to create parser for files %v, post-processing: %w", filenames, err)
		}

		for _, o := range output {
			if o.Output.Dst != nil {
				continue
			}
			fmt.Println("Re-parsing file ", o.Output.Filename)
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
		return fmt.Errorf("failed to write file %q: %w", output.Output.Filename, err)
	}

	return nil
}

// partitionByMode splits transformations into in-place and generated groups,
// preserving the original collection order within each group.
func partitionByMode(transformations []Transformation) (inplace, generated []Transformation) {
	for _, t := range transformations {
		if t.Transformer.Mode() == render.ModeInPlace {
			inplace = append(inplace, t)
		} else {
			generated = append(generated, t)
		}
	}
	return
}

func expandTransformations(
	ctx *contract.ShapingContext,
	cfg *Config,
	pkgs []*model.Package,
	transformations []Transformation,
) error {
	scope := render.Scope{}
	for _, t := range transformations {
		if err := t.Transformer.Expand(ctx, pkgs, t.Node, scope); err != nil {
			ctx.TransformerError(t.Transformer, t.Node, err)
			return nil
		}
		ctx.TransformerAdded(t.Transformer, t.Node)
	}
	return nil
}

// executeTransformations runs all transformations in dependency-safe order:
// in-place transformations first (each rendered and restored individually so that
// subsequent transformations see the on-disk changes), then generated
// transformations (grouped by output file), and finally queries.
func executeTransformations(
	ctx *contract.ShapingContext,
	cfg *Config,
	pkgs []*model.Package,
	transformations []Transformation,
) error {
	inplace, generated := partitionByMode(transformations)

	// In-place transformations run first, one at a time, restoring after each
	// so that later transformations can depend on the written output.
	for _, t := range inplace {
		outputs, err := renderTransformations(ctx, cfg, pkgs, []Transformation{t})
		if err != nil {
			return err
		}
		if len(outputs) > 0 {
			if err := restoreOutputs(ctx, outputs); err != nil {
				return err
			}
		}
	}

	// Generated transformations grouped by output file, restoring after each group.
	byOutput := lo.GroupBy(generated, func(t Transformation) string {
		return t.Transformer.Output()
	})
	for _, batch := range byOutput {
		outputs, err := renderTransformations(ctx, cfg, pkgs, batch)
		if err != nil {
			return err
		}
		if len(outputs) > 0 {
			if err := restoreOutputs(ctx, outputs); err != nil {
				return err
			}
		}
	}

	// Process queries last — they read from the (now up-to-date) packages
	// but do not feed back into other transformations.
	queryOutputs, err := processQueries(ctx, pkgs)
	if err != nil {
		return fmt.Errorf("failed to process queries: %w", err)
	}
	if len(queryOutputs) > 0 {
		var moList []*ManagerOutput
		for _, qo := range queryOutputs {
			moList = append(moList, &ManagerOutput{
				Output: &render.Output{
					Filename: qo.Filename,
					Package:  qo.Package,
					Dst: &render.DstOutput{
						File:    qo.File,
						Package: qo.Package.Package,
					},
				},
			})
		}
		return restoreOutputs(ctx, moList)
	}

	return nil
}

// buildPath constructs the output path information for a transformation based on its mode, package path, original filename,
// and desired output path.
func buildPath(mode, pkgPath, filename, output string) Pathinfo {
	var (
		baseDir        = path.Dir(filename)
		pkg            = path.Join(pkgPath, path.Dir(output))
		actualFilename = path.Join(baseDir, output)
	)

	if path.IsAbs(output) {
		actualFilename = output
		baseDir = path.Dir(output)

		pkg = pkgPath
	}

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

func buildModeManager(cfg *Config, pkg *model.Package, mode, output string) Manager {
	switch mode {
	case render.ModeInPlace:
		return NewInplaceManager(cfg, pkg, output)
	case render.ModeGenerated:
		return NewGeneratorManager(cfg, pkg, output)
	}
	return nil
}

// executeHandlers executes any handlers that were triggered by plumber:notify
// annotations during the run. The context's Notifications collector must be a
// *handler.Registry for execution to proceed.
func executeHandlers(ctx *contract.ShapingContext, cfg *Config) error {
	if ctx.Notifications == nil {
		return nil
	}
	return ctx.Notifications.Execute(ctx)
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
		case contract.TransformationShape:
			if err := changeTransformer(NewShaper(node, annotation)); err != nil {
				return nil, err
			}
			transformers = append(transformers, lastTransformer)
		case contract.TransformationDerive:
			if err := changeTransformer(NewDeriveTransformer(node, annotation)); err != nil {
				return nil, err
			}
			transformers = append(transformers, lastTransformer)
		case contract.TransformationRender:
			if err := changeTransformer(NewRenderTransformer(node, annotation)); err != nil {
				return nil, err
			}
			transformers = append(transformers, lastTransformer)
		default:
			if strings.HasPrefix(annotation.Name, "@") {
				// skip macro annotations during transformer building —
				// they are only for the expander to process, and should not be treated as transformation directives
				continue
			}
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
