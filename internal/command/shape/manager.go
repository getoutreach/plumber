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
	cfg     *ShapeConfig
}

func NewGeneratorManager(cfg *ShapeConfig, pkgPath, output string) *GeneratorManager {
	return &GeneratorManager{
		output:  output,
		pkgPath: pkgPath,
		cfg:     cfg,
	}
}

func managerRender(cfg *ShapeConfig, pkgPath string, opener gen.MemoryFileOpener, transformations []Transformation, scope map[string]any, output string) ([]*render.Output, error) {
	context := render.Context{
		PkgPath: pkgPath,
		Modules: render.NewModuleRegister(),
		Wrapper: NewTypeWrapper(cfg),
	}
	var contents []string

	err := runTransformations(context, gen.NewBufferFileOpener(), scope, transformations, func(content string) {
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
	return managerRender(m.cfg, m.pkgPath, opener, transformations, scope, m.output)
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
		context := render.Context{
			PkgPath: pkg.Path,
			Modules: modules,
			Wrapper: NewTypeWrapper(m.cfg),
		}

		var content string

		err := runTransformations(context, opener, scope, []Transformation{t}, func(c string) {
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
	state render.Context,
	opener gen.MemoryFileOpener,
	scope map[string]any,
	transformations []Transformation, contentFunc func(string),
) (err error) {
	for _, t := range transformations {
		ignores := state.Ignores
		if ignores == nil {
			ignores = render.NewIgnores(t.Transformer.GetAnnotations().FindAll(OptionIgnore).Values())
		}

		ctx := render.Context{
			Ignores: ignores,
			Modules: state.Modules,
			PkgPath: state.PkgPath,
			Wrapper: state.Wrapper,
		}

		fmt.Printf("  > Transformer[%s], Line: %d\n",
			t.Transformer.GetName(),
			t.Node.GetNode().GetPosition().Line,
		)

		scope["Subject"] = view.Annotable{
			Annotations: t.Transformer.GetAnnotations(),
		}

		content, err := t.Transformer.Render(ctx, t.Node.(*model.Type), scope, t.Transformer.Output(), opener)
		if err != nil {
			fmt.Println("Error during rendering:", err)
		}
		contentFunc(string(content))
	}
	return nil
}
