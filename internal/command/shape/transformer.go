// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Transformer interface and concrete ShapeTransformer and DeriveTransformer implementations for rendering annotated types.

package shape

import (
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

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
}

var defaultValues = map[string]string{
	contract.OptionMode: "generated",
}

var reSuffixed = regexp.MustCompile(`{suffix:([^}]+)}`)

type BasicTransformer struct {
	Position       model.Position
	Name           string
	AllowedOptions []string
	Options        model.Annotation
	Annotations    model.Annotations
}

func (t *BasicTransformer) GetName() string {
	return t.Name
}

func (t *BasicTransformer) Validate() error {
	if t.Annotations.Find(contract.OptionName) == nil {
		if len(t.Options.Args) > 0 {
			t.Annotations.Append(model.Annotation{
				Name: contract.OptionName,
				Args: t.Options.Args,
			})
		}
	}
	return nil
}

func (t *BasicTransformer) GetAnnotations() model.Annotations {
	return t.Annotations
}

func (t *BasicTransformer) Mode() string {
	a := t.Annotations.Find(contract.OptionMode)
	if a != nil {
		return a.Value()
	}
	return defaultValues[contract.OptionMode]
}

func (t *BasicTransformer) Output() string {
	if t.Mode() == "inplace" {
		return "inplace.go"
	}
	output := "generated.go"
	a := t.Annotations.Find(contract.OptionOutput)
	if a != nil {
		output = a.Value()
	}
	baseFilename := path.Base(t.Position.Filename)
	ext := path.Ext(baseFilename)

	name := strings.TrimSuffix(baseFilename, ext)

	output = strings.NewReplacer(
		"{filename}", baseFilename,
		"{name}", name,
		"{ext}", ext,
	).Replace(output)

	output = reSuffixed.ReplaceAllStringFunc(output, func(s string) string {
		// Extract the suffix value from the match
		matches := reSuffixed.FindStringSubmatch(s)
		if len(matches) > 1 {
			suffix := matches[1]
			return name + "_" + suffix + ext
		}
		return s
	})

	return output
}

func (t BasicTransformer) Accepts(annotation string) bool {
	return lo.Contains(t.AllowedOptions, annotation)
}

func (t *BasicTransformer) Add(annotation model.Annotation) {
	t.Annotations = append(t.Annotations, annotation)
}

type DeriveTransformer struct {
	BasicTransformer
}

type ShapeTransformer struct {
	BasicTransformer
}

func NewDeriveTransformer(pos model.Position, a model.Annotation) *DeriveTransformer {
	return &DeriveTransformer{
		BasicTransformer: BasicTransformer{
			Position:       pos,
			Name:           "derive",
			AllowedOptions: defaultOptions,
			Options:        a,
		},
	}
}

func (t *DeriveTransformer) Render(context render.Context, tp *model.Type, scope map[string]any, output string, opener gen.MemoryFileOpener) (string, error) {
	if tp.Struct == nil {
		return "", fmt.Errorf("derive transformer can only be applied to struct types, got %s", tp.Spec.Kind)
	}
	return render.Derive(context, tp, scope, output, opener)
}

func NewShapeTransformer(pos model.Position, a model.Annotation) *ShapeTransformer {
	return &ShapeTransformer{
		BasicTransformer: BasicTransformer{
			Position:       pos,
			Name:           "shape",
			AllowedOptions: defaultOptions,
			Options:        a,
		},
	}
}

func (t *ShapeTransformer) Render(context render.Context, tp *model.Type, scope map[string]any, output string, opener gen.MemoryFileOpener) (string, error) {
	if tp.Interface == nil && tp.Struct == nil {
		return "", fmt.Errorf("shape transformer can only be applied to interface or struct types, got %s", tp.Spec.Kind)
	}
	return render.Shape(context, tp, scope, output, opener)
}

type Annotable interface {
	GetAnnotations() model.Annotations
}

type Node interface {
	Annotable
	GetPosition() model.Position
}

type Transformer interface {
	Annotable
	Accepts(annotation string) bool
	Add(annotation model.Annotation)
	Validate() error
	Output() string
	GetName() string
	Mode() string
	Render(context render.Context, tp *model.Type, scope map[string]any, output string, opener gen.MemoryFileOpener) (string, error)
}

type Transformation struct {
	Node        model.Node
	Transformer Transformer
	Path        Pathinfo
}

type Pathinfo struct {
	Filename string
	RelPath  string
	BaseDir  string
	Package  string
}

type ManagerOutput struct {
	Output  *render.Output
	File    *dst.File
	Manager Manager
	Content []byte
}

type RenderFunc func(state render.Context, opener gen.MemoryFileOpener, transformations []Transformation, contentFunc func(string)) (err error)

type Manager interface {
	Render(pkgs []*model.Package, transformations []Transformation) ([]*render.Output, error)
	// Postprocess(output *ManagerOutput, content *dst.File, pkg *decorator.Package) error
}
