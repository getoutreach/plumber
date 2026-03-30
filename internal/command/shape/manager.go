package shape

import (
	"fmt"
	"path"

	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/internal/render/view"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

type GeneratorManager struct {
	output  string
	pkgPath string
}

func NewGeneratorManager(pkgPath, output string) *GeneratorManager {
	return &GeneratorManager{
		output:  output,
		pkgPath: pkgPath,
	}
}

func managerRender(pkgPath string, opener gen.MemoryFileOpener, transformations []Transformation, scope map[string]any, output string) ([]*render.Output, error) {
	context := render.Context{
		PkgPath: pkgPath,
		Modules: render.NewModuleRegister(),
	}
	var contents []string

	err := runTransformations(context, opener, transformations, func(content string) {
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

func (m *GeneratorManager) Render(_ []*model.Package, transformations []Transformation) ([]*render.Output, error) {
	var (
		scope = map[string]any{
			"Mode": "generated",
		}
		opener = gen.NewSystemFileOpener()
	)
	return managerRender(m.pkgPath, opener, transformations, scope, m.output)
}

// func (m *GeneratorManager) Postprocess(output *ManagerOutput, content *dst.File, pkg *decorator.Package) error {
// 	var buf bytes.Buffer
// 	r := decorator.NewRestorerWithImports("root", gopackages.New("./"))

// 	if err := r.Fprint(&buf, content); err != nil {
// 		return fmt.Errorf("failed to restore AST for file %q: %w", output.Output.Filename, err)
// 	}

// 	// Write to file
// 	if err := os.WriteFile(output.Output.Filename, buf.Bytes(), 0644); err != nil {
// 		return fmt.Errorf("failed to write file: %w", err)
// 	}

// 	return nil
// }

type InplaceManager struct {
	output  string
	pkgPath string
}

func NewInplaceManager(pkgPath, output string) *InplaceManager {
	return &InplaceManager{
		output:  output,
		pkgPath: pkgPath,
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
		context := render.Context{
			PkgPath: pkg.Path,
			Modules: modules,
		}

		var content string

		err := runTransformations(context, opener, []Transformation{t}, func(c string) {
			content = c
		})
		if err != nil {
			return nil, fmt.Errorf("error during rendering: %w", err)
		}
		//outout := path.Join(path.Dir(m.output), "inplacehelperpath", path.Base(m.output))

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

// func (m *InplaceManager) Postprocess(output *ManagerOutput, content *dst.File, pkg *decorator.Package) error {
// 	return nil
// }

func runTransformations(state render.Context, opener gen.MemoryFileOpener, transformations []Transformation, contentFunc func(string)) (err error) {
	for _, t := range transformations {
		ignores := state.Ignores
		if ignores == nil {
			ignores = render.NewIgnores(t.Transformer.GetAnnotations().FindAll("plumber:ignore").Values())
		}

		ctx := render.Context{
			Ignores: ignores,
			Modules: state.Modules,
			PkgPath: state.PkgPath,
		}

		fmt.Printf("  > Transformer[%s], Line: %d\n",
			t.Transformer.GetName(),
			t.Node.GetNode().GetPosition().Line,
		)
		//fmt.Println(t.Transformer.GetAnnotations())
		content, err := render.Derive(ctx, t.Node.(*model.Type), map[string]any{
			"Derive": view.Annotable{
				Annotations: t.Transformer.GetAnnotations(),
			},
		}, t.Transformer.Output(), opener)
		if err != nil {
			fmt.Println("Error during rendering:", err)
		}
		contentFunc(string(content))
	}
	return nil
}
