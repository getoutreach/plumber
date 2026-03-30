package shape

import (
	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

const (
	OptionTemplate = "plumber:template"
	OptionIgnore   = "plumber:ignore"
	OptionComment  = "plumber:comment"
	OptionName     = "plumber:name"
	OptionFilter   = "plumber:filter"
	OptionMixin    = "plumber:mixin"
	OptionOutput   = "plumber:output"
	OptionMode     = "plumber:mode"
)

var defaultOptions = []string{
	OptionTemplate,
	OptionIgnore,
	OptionComment,
	OptionName,
	OptionFilter,
	OptionMixin,
	OptionOutput,
	OptionMode,
}

var defaultValues = map[string]string{
	OptionMode: "generated",
}

type BasicTransformer struct {
	Name           string
	AllowedOptions []string
	Annotations    model.Annotations
}

func (t *BasicTransformer) GetName() string {
	return t.Name
}

func (t *BasicTransformer) Validate() error {
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

type DeriveTransformer struct {
	BasicTransformer
}

type ShapeTransformer struct {
	BasicTransformer
}

func NewDeriveTransformer() *DeriveTransformer {
	return &DeriveTransformer{
		BasicTransformer: BasicTransformer{
			Name:           "derive",
			AllowedOptions: defaultOptions,
		},
	}
}

func NewShapeTransformer() *ShapeTransformer {
	return &ShapeTransformer{
		BasicTransformer: BasicTransformer{
			Name:           "shape",
			AllowedOptions: defaultOptions,
		},
	}
}

func (t BasicTransformer) Accepts(annotation string) bool {
	return lo.Contains(t.AllowedOptions, annotation)
}

func (t *BasicTransformer) Add(annotation model.Annotation) {
	t.Annotations = append(t.Annotations, annotation)
}

type Transformer interface {
	Accepts(annotation string) bool
	Add(annotation model.Annotation)
	Validate() error
	Output() string
	GetName() string
	Mode() string
	GetAnnotations() model.Annotations
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
