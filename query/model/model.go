package model

import (
	"go/types"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/samber/lo"
)

type (
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
		Package *decorator.Package
		Name    string `json:"name" yaml:"name"`
		Path    string `json:"path" yaml:"path"`

		Types     []*Type     `json:"types" yaml:"types"`
		Functions []*Function `json:"functions" yaml:"functions"`
	}

	TypeSpec struct {
		FQN    string          `json:"fqn" yaml:"fqn"`
		Type   types.Type      `json:"-" yaml:"-"`
		Object *types.TypeName `json:"-" yaml:"-"`
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
