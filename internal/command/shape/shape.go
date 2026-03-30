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
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

func Run(config *ShapeConfig, args []string) error {
	filenames, err := inspect.ScanFiles("./", args)
	if err != nil {
		return fmt.Errorf("failed to scan files: %w", err)
	}

	pkgs, err := inspect.Inspect(filenames)
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
	for _, node := range transformingNodes {
		ts, err := buildTransformers(config, node)
		if err != nil {
			return fmt.Errorf("failed to build transformers for node %q: %w", node.GetNode().GetPosition(), err)
		}
		for _, t := range ts {
			transformations = append(transformations, Transformation{
				Node:        node,
				Transformer: t,
				Path:        buildPath(t.Mode(), node.GetPackage().Path, node.GetNode().GetPosition().Filename, t.Output()),
			})
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
			manager := buildModeManager(mode, transformations[0].Path.Package, filename)

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

	// restore dst files out of augment dst structures
	for _, o := range output {
		if o.Output.Dst == nil {
			continue
		}
		err = restoreOutput(o, o.Output.Dst.File, o.Output.Dst.Package)
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

	switch mode {
	case "inplace":
		return Pathinfo{
			Filename: baseDir,
			BaseDir:  baseDir,
			RelPath:  "",
			Package:  pkgPath,
		}
	default:
		return Pathinfo{
			Filename: path.Join(baseDir, output),
			BaseDir:  baseDir,
			RelPath:  output,
			Package:  path.Join(pkgPath, path.Dir(output)),
		}
	}
}

func buildModeManager(mode string, pkgPath string, output string) Manager {
	switch mode {
	case "inplace":
		return NewInplaceManager(pkgPath, output)
	case "generated":
		return NewGeneratorManager(pkgPath, output)
	}
	return nil

}

func buildTransformers(config *ShapeConfig, node model.Node) (transformers []Transformer, err error) {
	var (
		lastTransformer Transformer
	)
	transformers = []Transformer{}
	for _, annotation := range node.GetNode().GetAnnotations() {
		switch annotation.Name {
		case "plumber:shape":
			lastTransformer = NewShapeTransformer()
			transformers = append(transformers, lastTransformer)
		case "plumber:derive":
			lastTransformer = NewDeriveTransformer()
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
					fmt.Println(config.Mixins)
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
	return transformers, nil
}
