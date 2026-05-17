// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Transformer interface and concrete Shaper and
// DeriveTransformer implementations for rendering annotated types.

package shape

import (
	"fmt"
	"path"
	"strings"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/expand"
	"github.com/getoutreach/plumber/internal/command/shape/render"
	"github.com/getoutreach/plumber/internal/genius/gen"
	baserender "github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// defaultOptions defines the set of annotation options that are allowed for shape and derive transformers.
var defaultOptions = []string{
	contract.OptionTemplate,
	contract.OptionIgnore,
	contract.OptionContext,
	contract.OptionComment,
	contract.OptionName,
	contract.OptionFilter,
	contract.OptionMixin,
	contract.OptionOutput,
	contract.OptionMode,
	contract.OptionFieldWrapper,
	contract.OptionReceiver,
	contract.OptionScope,
	contract.OptionDependsOn,
	contract.OptionNotify,
}

// Transformer defines the interface for transforming annotated nodes in the AST, such as struct types,
// into generated code based on specified annotations and options.
type Transformer interface {
	contract.Annotable
	contract.Transformer
	// Accepts(annotation string) bool
	// Add(annotation model.Annotation)
	// Validate(packager contract.Packager) error
	// GetPosition() model.Position
	GetOptions() model.Annotation
	// Output() string
	// GetName() string
	// Mode() string
	Expand(ctx *contract.ShapingContext, pkgs []*model.Package, node model.Node, scope baserender.Scope) error
	Render(context *render.Context, tp *model.Type, scope baserender.Scope, output string, opener gen.MemoryFileOpener) (string, error)
}

// defaultValues defines the default values for certain annotation options when they are not explicitly provided.
var defaultValues = map[string]string{
	contract.OptionMode: "generated",
}

// Node represents a node in the AST that can be transformed by a Transformer, such as a struct or interface type.
type Node interface {
	contract.Annotable
	GetPosition() model.Position
}

// Transformation represents a single transformation to be applied to a node,
// including the node itself, the transformer to be applied, and the path information for the output file.
type Transformation struct {
	Node        model.Node
	Transformer Transformer
	//Path        Pathinfo
}

// Pathinfo represents the information about the output path for a generated file,
// including the filename, relative path, base directory, and package name.
type Pathinfo struct {
	Filename string
	RelPath  string
	BaseDir  string
	Package  string
}

// ManagerOutput represents the output of a Manager's Render method,
// including the generated output, the corresponding dst.File, the Manager that produced it, and the raw content bytes.
type ManagerOutput struct {
	Output  *baserender.Output
	File    *dst.File
	Manager Manager
	Content []byte
}

// RenderFunc defines the function signature for rendering transformations,
// which takes a render context, a memory file opener, a list of transformations,
// and a content function for handling generated content. It returns an error if the rendering process fails.
type RenderFunc func(
	state *render.Context,
	opener gen.MemoryFileOpener,
	transformations []Transformation,
	contentFunc func(string),
) (err error)

// Manager defines the interface for managing the rendering of transformations,
type Manager interface {
	Render(ctx *contract.ShapingContext, pkgs []*model.Package, transformations []Transformation) ([]*baserender.Output, error)
}

// BasicTransformer provides a base implementation of the Transformer interface,
// handling common annotation processing and output path generation logic for shape and derive transformers.
type BasicTransformer struct {
	Position       model.Position
	Package        *model.Package
	Name           string
	AllowedOptions []string
	Options        model.Annotation
	Annotations    model.Annotations
}

func (t *BasicTransformer) GetName() string {
	return t.Name
}

func (t *BasicTransformer) GetPosition() model.Position {
	return t.Position
}

func (t *BasicTransformer) GetPackage() *model.Package {
	return t.Package
}

func (t *BasicTransformer) GetOptions() model.Annotation {
	return t.Options
}

// Validate checks if the transformer has the required annotations and options,
// and adds default annotations if necessary.
func (t *BasicTransformer) Validate(packager contract.Packager) error {
	if t.Annotations.Find(contract.OptionName) == nil {
		if len(t.Options.Args) > 0 {
			t.Annotations.Append(model.NewAnnotation(
				contract.OptionName,
				t.Options.Args,
				model.WithNamedArgs(t.Options.NamedArgs),
				model.WithOptionalImpliedBy(t.Options.ImpliedBy),
			))
		}
	}

	if t.Annotations.Find(contract.OptionOutput) == nil {
		t.Annotations.Append(model.NewAnnotation(
			contract.OptionOutput,
			[]string{"generated.go"},
		))
	}
	// path.Join(node.GetPackage().Dir

	return nil
}

func (t *BasicTransformer) GetAnnotations() model.Annotations {
	return t.Annotations
}

func (t *BasicTransformer) Mode() string {
	return t.Annotations.Find(contract.OptionMode).ValueOr(defaultValues[contract.OptionMode])
}

// Output generates the output filename for the transformed code based on the transformer configuration and the
// position of the annotated node.
//
// The returned filename is used as the final destination for the generated/derived code.
// In inplace mode this is the file the synthesized declarations are merged or appended into;
// the intermediate fragment used during template rendering is independent of this value.
func (t *BasicTransformer) Output() string {
	return t.Annotations.Find(contract.OptionOutput).Value()
}

func (t *BasicTransformer) Accepts(annotation string) bool {
	return lo.Contains(t.AllowedOptions, annotation)
}

func (t *BasicTransformer) Add(annotation model.Annotation) {
	t.Annotations = append(t.Annotations, annotation)
}

func (t *BasicTransformer) Expand(ctx *contract.ShapingContext, pkgs []*model.Package, node model.Node, scope baserender.Scope) error {
	annotations, err := expand.TransformerAnnotations(node, t.Annotations, scope)
	if err != nil {
		return err
	}
	t.Annotations = annotations

	// Path
	if output := t.Annotations.Find(contract.OptionOutput); output != nil {
		value := output.Value()
		if !strings.HasPrefix(value, "/") {
			dir := path.Dir(t.Position.Filename)
			if dir == "" || dir == "." {
				dir = node.GetPackage().Dir
			}
			value = strings.TrimPrefix(value, "./")
			value = path.Join(dir, value)
			output.SetValue(path.Clean(value))
		}
	}

	dir := path.Dir(t.Output())

	pkgPath, err := ctx.DeriveModulePath(dir)
	if err != nil {
		return fmt.Errorf("can't determinate package path based on dir: %q: %w", dir, err)
	}

	pkg, found := lo.Find(pkgs, func(p *model.Package) bool {
		fmt.Println(p.Path)
		return p.Path == pkgPath
	})
	if !found {
		return fmt.Errorf("can't find output package based on path: %s, derived package path: %s", dir, pkgPath)
	}
	t.Package = pkg

	return nil
}
