package shape

import (
	"fmt"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

const (
	OptionTemplate     = "plumber:template"
	OptionIgnore       = "plumber:ignore"
	OptionContext      = "plumber:context"
	OptionComment      = "plumber:comment"
	OptionName         = "plumber:name"
	OptionFilter       = "plumber:filter"
	OptionMixin        = "plumber:mixin"
	OptionOutput       = "plumber:output"
	OptionMode         = "plumber:mode"
	OptionFieldWrapper = "plumber:field_wrapper"
)

var defaultOptions = []string{
	OptionTemplate,
	OptionIgnore,
	OptionContext,
	OptionComment,
	OptionName,
	OptionFilter,
	OptionMixin,
	OptionOutput,
	OptionMode,
	OptionFieldWrapper,
}

var defaultValues = map[string]string{
	OptionMode: "generated",
}

type BasicTransformer struct {
	Name           string
	AllowedOptions []string
	Options        model.Annotation
	Annotations    model.Annotations
}

func (t *BasicTransformer) GetName() string {
	return t.Name
}

func (t *BasicTransformer) Validate() error {
	if t.Annotations.Find(OptionName) == nil {
		if len(t.Options.Args) > 0 {
			t.Annotations.Append(model.Annotation{
				Name: OptionName,
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
	a := t.Annotations.Find(OptionMode)
	if a != nil {
		return a.Value()
	}
	return defaultValues[OptionMode]
}

func (t *BasicTransformer) Output() string {
	if t.Mode() == "inplace" {
		return "inplace.go"
	}
	a := t.Annotations.Find(OptionOutput)
	if a != nil {
		return a.Value()
	}
	return "generated/generated.go"
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

func NewDeriveTransformer(a model.Annotation) *DeriveTransformer {
	return &DeriveTransformer{
		BasicTransformer: BasicTransformer{
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

func NewShapeTransformer(a model.Annotation) *ShapeTransformer {
	return &ShapeTransformer{
		BasicTransformer: BasicTransformer{
			Name:           "shape",
			AllowedOptions: defaultOptions,
			Options:        a,
		},
	}
}

func (t *ShapeTransformer) Render(context render.Context, tp *model.Type, scope map[string]any, output string, opener gen.MemoryFileOpener) (string, error) {
	if tp.Interface == nil {
		return "", fmt.Errorf("shape transformer can only be applied to interface types, got %s", tp.Spec.Kind)
	}
	return render.Shape(context, tp, scope, output, opener)
}

type Annotable interface {
	GetAnnotations() model.Annotations
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
