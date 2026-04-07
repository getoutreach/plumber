package model

import (
	"go/types"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/samber/lo"
)

type (
	Kind     string
	Category string
	Family   string
	Position struct {
		Filename string `json:"filename" yaml:"filename"`
		Line     int    `json:"line" yaml:"line"`
		Column   int    `json:"column" yaml:"column"`
	}

	AnnotationProvider interface {
		GetAnnotations() Annotations
	}

	Annotation struct {
		Name      string            `json:"name" yaml:"name"`
		Args      []string          `json:"args,omitempty" yaml:"args,omitempty"`
		NamedArgs map[string]string `json:"namedArgs,omitempty" yaml:"namedArgs,omitempty"`
	}

	Annotations []Annotation

	TypeNode struct {
		Package     *Package    `json:"-" yaml:"-"`
		Position    Position    `json:"position" yaml:"position,omitempty"`
		Doc         string      `json:"doc,omitempty" yaml:"doc,omitempty"`
		Annotations Annotations `json:"annotations,omitempty" yaml:"annotations,omitempty"`
	}

	Node interface {
		GetNode() *TypeNode
		GetPackage() *Package
	}

	Package struct {
		Package *decorator.Package `json:"-" yaml:"-"`
		Name    string             `json:"name" yaml:"name"`
		Path    string             `json:"path" yaml:"path"`

		Types     []*Type         `json:"types" yaml:"types"`
		Functions []*Function     `json:"functions" yaml:"functions"`
		Comments  []*CommentGroup `json:"comments" yaml:"comments"`
	}

	Packages []*Package

	CommentGroup struct {
		Doc         string      `json:"doc,omitempty" yaml:"doc,omitempty"`
		Annotations Annotations `json:"annotations,omitempty" yaml:"annotations,omitempty"`
	}

	TypeSpec struct {
		TypeKind `json:",inline" yaml:",inline"`
		FQN      string          `json:"fqn" yaml:"fqn"`
		Type     types.Type      `json:"-" yaml:"-"`
		Object   *types.TypeName `json:"-" yaml:"-"`
	}

	TypeKind struct {
		Kind        Kind      `json:"kind" yaml:"kind,omitempty"`
		Key         *TypeKind `json:"key" yaml:"key,omitempty"`
		Elem        *TypeKind `json:"elem" yaml:"elem,omitempty"`
		Underlaying *TypeKind `json:"underlaying" yaml:"underlaying,omitempty"`
	}

	Type struct {
		*TypeNode `json:",inline" yaml:",inline"`
		Spec      TypeSpec   `json:"spec" yaml:"spec"`
		Name      string     `json:"name,omitempty" yaml:"name,omitempty"`
		Function  *Function  `json:"function,omitempty" yaml:"function,omitempty"`
		Struct    *Struct    `json:"struct,omitempty" yaml:"struct,omitempty"`
		Interface *Interface `json:"interface,omitempty" yaml:"interface,omitempty"`
	}

	TypeDefinition struct {
		Spec TypeSpec `json:"spec" yaml:"spec"`
	}

	Tag struct {
		Name  string `json:"name" yaml:"name"`
		Value string `json:"value" yaml:"value"`
	}

	Var struct {
		Name        string          `json:"name,omitempty" yaml:"name,omitempty"`
		Doc         string          `json:"doc,omitempty" yaml:"doc,omitempty"`
		Annotations Annotations     `json:"annotations,omitempty" yaml:"annotations,omitempty"`
		Type        *TypeDefinition `json:"type" yaml:"type"`
		Tags        []Tag
	}

	Function struct {
		TypeNode `json:",inline" yaml:",inline"`
		Name     string `json:"name" yaml:"name"`
		Receiver *Var   `json:"receiver,omitempty" yaml:"receiver,omitempty"`
		Args     []*Var `json:"arguments,omitempty" yaml:"args,omitempty"`
		Results  []*Var `json:"results,omitempty" yaml:"returns,omitempty"`
	}

	Interface struct {
		Interface *types.Interface `json:"-" yaml:"-"`
		Methods   []*Function      `json:"methods,omitempty" yaml:"methods,omitempty"`
	}

	Struct struct {
		Struct  *types.Struct `json:"-" yaml:"-"`
		Methods []*Function   `json:"methods,omitempty" yaml:"methods,omitempty"`
		Fields  []*Var        `json:"fields,omitempty" yaml:"fields,omitempty"`
	}
)

const (
	KindStruct    Kind = "struct"
	KindInterface Kind = "interface"
	KindFunc      Kind = "func"
	KindMap       Kind = "map"
	KindSlice     Kind = "slice"
	KindArray     Kind = "array"
	KindPointer   Kind = "pointer"
	KindChan      Kind = "chan"
	KindString    Kind = "string"
	KindInt       Kind = "int"
	KindFloat     Kind = "float"
	KindBool      Kind = "bool"
)

func (p Packages) TypeByFQN(fqn *astx.FQN) *Type {
	s := fqn.String()
	for _, pkg := range p {
		for _, t := range pkg.Types {
			if t.Spec.FQN == s {
				return t
			}
		}
	}
	return nil
}

func (p *Package) File(filename string) *dst.File {
	for _, f := range p.Package.Syntax {
		if p.Package.Decorator.Filenames[f] == filename {
			return f
		}
	}
	return nil
}

func NewAnnotation(name string, args ...string) Annotation {
	return Annotation{Name: name, Args: args}
}

func (aa Annotations) Find(name string) *Annotation {
	for _, ann := range aa {
		if ann.Name == name {
			return &ann
		}
	}
	return nil
}

func (aa Annotations) FindAll(name string) Annotations {
	var matches Annotations
	for _, ann := range aa {
		if ann.Name == name {
			matches = append(matches, ann)
		}
	}
	return matches
}

func (aa *Annotations) Append(a Annotation) *Annotations {
	*aa = append(*aa, a)
	return aa
}

func (aa Annotations) Values() []string {
	return lo.Map(aa, func(a Annotation, _ int) string {
		return a.Value()
	})
}

func (n *TypeNode) GetNode() *TypeNode {
	return n
}

func (n *TypeNode) GetPackage() *Package {
	return n.Package
}

func (n *TypeNode) GetPosition() Position {
	return n.Position
}

func (n *TypeNode) GetDoc() string {
	return n.Doc
}

func (n *TypeNode) GetAnnotations() Annotations {
	return n.Annotations
}

func (n *Var) GetAnnotations() Annotations {
	return n.Annotations
}

func (m *Annotation) Value() string {
	if len(m.Args) > 0 {
		return m.Args[0]
	}
	return ""
}

func (n *CommentGroup) GetAnnotations() Annotations {
	return n.Annotations
}

func (n *CommentGroup) FilterAnnotations(expressions ...func(a Annotation) bool) *CommentGroup {
	aa := lo.Filter(n.Annotations, func(a Annotation, _ int) bool {
		for _, expr := range expressions {
			if !expr(a) {
				return false
			}
		}
		return true
	})
	return &CommentGroup{
		Annotations: aa,
	}
}

func NewTypeSpec(fqn *astx.FQN, t types.Type) TypeSpec {
	return TypeSpec{
		TypeKind: buildTypeKind(fqn, t),
		FQN:      fqn.String(),
		Type:     t,
	}
}

func (t TypeKind) Indirect() *TypeKind {
	if t.Kind == "pointer" {
		return t.Elem
	}
	return &t
}

func buildTypeKind(fqn *astx.FQN, t types.Type) TypeKind {
	if t == nil {
		return TypeKind{}
	}
	return typeKindFromType(t)
}

// typeKindFromType recursively maps a go/types.Type into a TypeKind tree,
// filling Key/Elem for composite types and Underlaying for named types.
func typeKindFromType(t types.Type) TypeKind {
	switch v := t.(type) {
	case *types.Basic:
		return TypeKind{Kind: basicTypeKind(v)}

	case *types.Named:
		// The underlying type of a named type is always an unnamed type, so
		// recursing here will never re-enter this case (no infinite loop).
		underlying := v.Underlying()
		underlyingTK := typeKindFromType(underlying)
		return TypeKind{
			Kind:        underlyingTK.Kind,
			Key:         underlyingTK.Key,
			Elem:        underlyingTK.Elem,
			Underlaying: &underlyingTK,
		}

	case *types.Pointer:
		elem := typeKindFromType(v.Elem())
		return TypeKind{Kind: KindPointer, Elem: &elem}

	case *types.Slice:
		elem := typeKindFromType(v.Elem())
		return TypeKind{Kind: KindSlice, Elem: &elem}

	case *types.Array:
		elem := typeKindFromType(v.Elem())
		return TypeKind{Kind: KindArray, Elem: &elem}

	case *types.Map:
		key := typeKindFromType(v.Key())
		elem := typeKindFromType(v.Elem())
		return TypeKind{Kind: KindMap, Key: &key, Elem: &elem}

	case *types.Chan:
		elem := typeKindFromType(v.Elem())
		return TypeKind{Kind: KindChan, Elem: &elem}

	case *types.Interface:
		return TypeKind{Kind: KindInterface}

	case *types.Struct:
		return TypeKind{Kind: KindStruct}

	case *types.Signature:
		return TypeKind{Kind: KindFunc}

	default:
		return TypeKind{}
	}
}

// basicTypeKind maps a *types.Basic to one of the scalar Kind constants.
func basicTypeKind(b *types.Basic) Kind {
	switch {
	case b.Info()&types.IsString != 0:
		return KindString
	case b.Info()&types.IsInteger != 0:
		return KindInt
	case b.Info()&types.IsFloat != 0:
		return KindFloat
	case b.Info()&types.IsBoolean != 0:
		return KindBool
	default:
		// Covers complex numbers, unsafe.Pointer, etc. – use the type name.
		return Kind(b.Name())
	}
}

func (k Kind) String() string {
	return string(k)
}
