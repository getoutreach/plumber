// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the shape command runner, orchestrating annotation discovery, transformer building, and output restoration.

// Package shape implements the internal logic for the plumber shape command, transforming annotated Go types into generated or inplace output files.
package shape

import (
	"bytes"
	"fmt"
	"os"
	"path"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/dave/dst/decorator/resolver/gopackages"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/astx/inspect"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/templates"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

func Run(config *ShapeConfig, args []string) error {
	err := templates.Checkout(&config.Templates, config.CacheDir)
	if err != nil {
		return fmt.Errorf("failed to checkout templates: %w", err)
	}

	filenames, err := inspect.ScanFiles("./", args)
	if err != nil {
		return fmt.Errorf("failed to scan files: %w", err)
	}

	pkgs, err := inspect.Inspect(filenames, "./")
	if err != nil {
		return fmt.Errorf("failed to inspect files: %w", err)
	}

	var (
		transformingNodes = []model.Node{}
	)

	inspect.Walk(pkgs, inspect.WithAnnotations(
		inspect.WithAnnotationName("plumber:shape", "plumber:derive"),
		func(node model.Node) error {
			transformingNodes = append(transformingNodes, node)
			return nil
		}))

	var transformations []Transformation

	appendTransformer := func(node model.Node, ts []Transformer) {
		for _, t := range ts {
			transformations = append(transformations, Transformation{
				Node:        node,
				Transformer: t,
				Path:        buildPath(t.Mode(), node.GetPackage().Path, node.GetNode().GetPosition().Filename, t.Output()),
			})
		}
	}

	for _, node := range transformingNodes {
		ts, err := buildTransformers(config, node.GetNode())
		if err != nil {
			return fmt.Errorf("failed to build transformers for node %q: %w", node.GetNode().GetPosition(), err)
		}
		appendTransformer(node, ts)
	}

	// process package-level comments for transformations
	for _, pkg := range pkgs {
		for _, comment := range pkg.Comments {
			m := comment.Annotations.Find(contract.OptionContext)
			if m == nil {
				continue
			}
			fqn, err := astx.ParseFQN(m.Value())
			if err != nil {
				return fmt.Errorf("failed to parse model FQN %q: %w", m.Value(), err)
			}
			t := pkgs.TypeByFQN(fqn)
			if t == nil {
				return fmt.Errorf("model type %q not found in packages", fqn)
			}
			ts, err := buildTransformers(config, comment.FilterAnnotations(func(a model.Annotation) bool {
				return a.Name != contract.OptionContext
			}))
			if err != nil {
				return fmt.Errorf("failed to build transformers for node %q: %w", t.GetNode().GetPosition(), err)
			}
			appendTransformer(t, ts)
		}
	}

	byMode := lo.GroupBy(transformations, func(t Transformation) string {
		return t.Transformer.Mode()
	})

	outputs := []*ManagerOutput{}

	for mode, transformations := range byMode {
		fmt.Printf("Processing mode %q with %d transformations\n", mode, len(transformations))

		byOutput := lo.GroupBy(transformations, func(t Transformation) string {
			return t.Path.Filename
		})

		for filename, transformations := range byOutput {
			manager := buildModeManager(config, mode, transformations[0].Path.Package, filename)

			fmt.Printf("Found %d transformations for output %q\n", len(transformations), filename)

			output, err := manager.Render(pkgs, transformations)
			if err != nil {
				return fmt.Errorf("failed to render transformations for output %q: %w", filename, err)
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
	if len(outputs) > 0 {
		return restoreOutputs(outputs)
	}

	return nil
}

func restoreOutputs(output []*ManagerOutput) error {
	filenames := []string{}
	overlay := make(map[string][]byte)

	// restore generated files out of rendered output
	for _, o := range output {
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
			err = restoreOutput(o, content, pkg)
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
		err := restoreOutput(o, o.Output.Dst.File, o.Output.Dst.Package)
		if err != nil {
			return fmt.Errorf("failed to restore generated dst file %q: %w", o.Output.Filename, err)
		}
	}

	return nil
}

func restoreOutput(output *ManagerOutput, content *dst.File, pkg *decorator.Package) error {
	fmt.Println("Restoring", output.Output.Filename)
	var buf bytes.Buffer
	r := decorator.NewRestorerWithImports(pkg.PkgPath, gopackages.WithHints("./", map[string]string{"time": "time"}))

	if err := r.Fprint(&buf, content); err != nil {
		return fmt.Errorf("failed to restore AST for file %q: %w", output.Output.Filename, err)
	}

	// Write to file
	if err := os.WriteFile(output.Output.Filename, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return nil
}

func buildPath(mode, pkgPath, filename, output string) Pathinfo {
	baseDir := path.Dir(filename)

	actualFilename := path.Join(baseDir, output)

	pkg := path.Join(pkgPath, path.Dir(output))

	if mode == "inplace" {
		actualFilename = pkg
	}

	return Pathinfo{
		Filename: actualFilename,
		BaseDir:  baseDir,
		RelPath:  output,
		Package:  pkg,
	}
}

func buildModeManager(cfg *ShapeConfig, mode string, pkgPath string, output string) Manager {
	switch mode {
	case "inplace":
		return NewInplaceManager(cfg, pkgPath, output)
	case "generated":
		return NewGeneratorManager(cfg, pkgPath, output)
	}
	return nil

}

func buildTransformers(config *ShapeConfig, node Node) (transformers []Transformer, err error) {
	var (
		lastTransformer Transformer
	)

	changeTransformer := func(t Transformer) error {
		if lastTransformer != nil {
			if err := lastTransformer.Validate(); err != nil {
				return err
			}
		}
		if t != nil {
			lastTransformer = t
		}
		return nil
	}

	transformers = []Transformer{}
	for _, annotation := range node.GetAnnotations() {
		switch annotation.Name {
		case "plumber:shape":
			if err := changeTransformer(NewShapeTransformer(node.GetPosition(), annotation)); err != nil {
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
			if annotation.Name == "plumber:mixin" {
				mixinName := annotation.Value()
				mixinConfig, ok := lo.Find(config.Mixins, func(mixin MixinConfig) bool {
					return mixin.PlumberMixin != nil && mixin.PlumberMixin.Name == mixinName
				})
				if !ok {
					return nil, fmt.Errorf("mixin %q not found in config", mixinName)
				}
				for _, mixinAnnotation := range mixinConfig.PlumberMixin.Annotations {
					if !lastTransformer.Accepts(mixinAnnotation.Name) {
						return nil, fmt.Errorf("transformer %s does not accept annotation %q from mixin %q", lastTransformer.GetName(), mixinAnnotation.Name, mixinName)
					}
					a := model.NewAnnotation(mixinAnnotation.Name, mixinAnnotation.Args...)
					a.NamedArgs = mixinAnnotation.NamedArgs
					lastTransformer.Add(a)
				}
			}
		}
	}
	if err := changeTransformer(nil); err != nil {
		return nil, err
	}
	return transformers, nil
}
