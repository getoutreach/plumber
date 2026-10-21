// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the core AST data model for the plumber pipeline including Package,
// Type, Annotation, TypeKind, Function, Struct, and Interface types.

// Package model defines the core data model representing Go packages, types, annotations,
// and functions discovered by the plumber AST inspection pipeline.
package model

import (
	"fmt"
	"go/types"
	"path/filepath"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/astx"

	"github.com/samber/lo"
)

// Types
type (
	// Kind represents the kind of a type, such as struct, interface, func, map, slice,
	// array, pointer, chan, or basic types like string, int, etc.
	Kind string

	// Is represents a set of boolean flags indicating the properties of a type,
	Is struct {
		Boolean  bool `json:"isBoolean" yaml:"boolean"`
		Integer  bool `json:"isInteger" yaml:"integer"`
		Unsigned bool `json:"isUnsigned" yaml:"unsigned"`
		Float    bool `json:"isFloat" yaml:"float"`
		Complex  bool `json:"isComplex" yaml:"complex"`
		String   bool `json:"isString" yaml:"string"`
		Untyped  bool `json:"isUntyped" yaml:"untyped"`

		Ordered   bool `json:"isOrdered" yaml:"ordered"`
		Numeric   bool `json:"isNumeric" yaml:"numeric"`
		ConstType bool `json:"isConstType" yaml:"const_type"`
	}

	// Position represents the position of a node in the source code.
	Position struct {
		Filename string `json:"filename" yaml:"filename"`
		Line     int    `json:"line" yaml:"line"`
		Column   int    `json:"column" yaml:"column"`
	}

	// AnnotationProvider is an interface for nodes that can provide annotations.
	AnnotationProvider interface {
		GetAnnotations() Annotations
	}

	// Annotation represents a metadata annotation that can be attached to a node.
	Annotation struct {
		Name      string            `json:"name" yaml:"name"`
		Args      []string          `json:"args,omitempty" yaml:"args,omitempty"`
		NamedArgs map[string]string `json:"namedArgs,omitempty" yaml:"namedArgs,omitempty"`
		// ImpliedBy is a reference to the annotation that implied this annotation (macro, mixin), if any.
		ImpliedBy *Annotation `json:"-" yaml:"-"`
		// DocLine is the 1-indexed line number within the raw doc comment string where this
		// annotation was parsed from. It is used for line-based filtering of transformations.
		DocLine int `json:"-" yaml:"-"`

		Position *Position `json:"position,omitempty" yaml:"position,omitempty"`

		Source Node `json:"-" yaml:"-"`
	}

	// Annotations is a slice of Annotation, providing utility methods for searching and filtering annotations.
	Annotations []Annotation

	// TypeNode represents a node in the type system, including its package, position, documentation, and annotations.
	TypeNode struct {
		Package     *Package    `json:"-" yaml:"-"`
		Position    Position    `json:"position" yaml:"position,omitempty"`
		Doc         string      `json:"doc,omitempty" yaml:"doc,omitempty"`
		Annotations Annotations `json:"annotations,omitempty" yaml:"annotations,omitempty"`
	}

	// Node is an interface implemented by all AST nodes in the model, providing access to the underlying TypeNode and its associated package.
	Node interface {
		GetAnnotations() Annotations
		// GetNode() *TypeNode
		GetPackage() *Package
		GetPosition() Position
		GetDoc() string
	}

	// Package represents a Go package, containing its name, path, types, functions, variables, and comments.
	Package struct {
		Package *decorator.Package `json:"-" yaml:"-"`
		Name    string             `json:"name" yaml:"name"`
		// Path represents the import path of the package, such as "github.com/getoutreach/plumber/query/model".
		Path string `json:"path" yaml:"path"`
		// Dir represents the directory of the package, such as "/home/user/go/src/github.com/getoutreach/plumber/query/model".
		Dir string `json:"dir" yaml:"dir"`

		Types     []*Type         `json:"types" yaml:"types"`
		Functions []*Function     `json:"functions" yaml:"functions"`
		Vars      []*PackageVar   `json:"vars,omitempty" yaml:"vars,omitempty"`
		Consts    []*PackageConst `json:"consts,omitempty" yaml:"consts,omitempty"`
		Comments  []*CommentGroup `json:"comments" yaml:"comments"`
	}

	// PackageVar represents a package-level variable declaration, including its name, type, documentation, annotations, and position.
	PackageVar struct {
		TypeNode `json:",inline" yaml:",inline"`
		Name     string          `json:"name" yaml:"name"`
		Type     *TypeDefinition `json:"type" yaml:"type"`
		VarType  types.Type      `json:"-" yaml:"-"`
	}

	// PackageConst represents a package-level constant declaration, including its name,
	// type, documentation, annotations, and position. It mirrors PackageVar so that the
	// inspect pipeline, annotation expansion, and inplace merge logic can treat
	// constants uniformly alongside variables.
	PackageConst struct {
		TypeNode  `json:",inline" yaml:",inline"`
		Name      string          `json:"name" yaml:"name"`
		Type      *TypeDefinition `json:"type" yaml:"type"`
		ConstType types.Type      `json:"-" yaml:"-"`
	}

	// Packages is a collection of Package, providing utility methods for looking up types by their fully qualified name (FQN).
	Packages []*Package

	// CommentGroup represents a group of comments associated with a node, including the comment text, annotations, and position.
	CommentGroup struct {
		Doc         string      `json:"doc,omitempty" yaml:"doc,omitempty"`
		Annotations Annotations `json:"annotations,omitempty" yaml:"annotations,omitempty"`
		Position    Position
		Package     *Package `json:"-" yaml:"-"`
	}

	// TypeSpec represents the specification of a type, including its kind (struct, interface, func, etc.),
	// fully qualified name (FQN), underlying go/types.Type, and associated *types.TypeName object for named types.
	TypeSpec struct {
		TypeKind `json:",inline" yaml:",inline"`
		FQN      string          `json:"fqn" yaml:"fqn"`
		Type     types.Type      `json:"-" yaml:"-"`
		Object   *types.TypeName `json:"-" yaml:"-"`
	}

	// TypeKind holds information about the kind of a type
	// (struct, interface, func, map, slice, array, pointer, chan, or basic types)
	// and its key/element types for composite types.
	//
	// Generic information:
	//   - Instantiated generic types (`mapping.Schema[entity.Field, postgres.Column]`) carry
	//     Origin (FQN of the generic declaration, e.g. `"pkg".Schema`) and TypeArgs.
	//   - Generic declarations (`type Schema[K comparable, V any] struct{...}`) carry TypeParams.
	//   - Usages of a type parameter (a field typed `K` inside a generic struct) have
	//     Kind == KindTypeParam and carry Param.
	TypeKind struct {
		Is          Is        `json:"is" yaml:"is,omitempty"`
		Kind        Kind      `json:"kind" yaml:"kind,omitempty"`
		Key         *TypeKind `json:"key" yaml:"key,omitempty"`
		Elem        *TypeKind `json:"elem" yaml:"elem,omitempty"`
		Underlaying *TypeKind `json:"underlaying" yaml:"underlaying,omitempty"`

		// Origin is the FQN of the generic declaration this type instantiates
		// (empty for non-instantiated types).
		Origin string `json:"origin,omitempty" yaml:"origin,omitempty"`
		// TypeArgs are the type arguments of an instantiated generic type, in order.
		TypeArgs []*TypeSpec `json:"typeArgs,omitempty" yaml:"typeArgs,omitempty"`
		// TypeParams are the type parameters of a generic declaration, in order.
		TypeParams []*TypeParam `json:"typeParams,omitempty" yaml:"typeParams,omitempty"`
		// Param describes the type parameter when Kind == KindTypeParam.
		Param *TypeParam `json:"param,omitempty" yaml:"param,omitempty"`
	}

	// TypeParam describes a type parameter of a generic type or function, e.g. `K comparable`.
	TypeParam struct {
		// Name is the parameter name (`K`).
		Name string `json:"name" yaml:"name"`
		// Index is the position of the parameter in its declaring list.
		Index int `json:"index" yaml:"index"`
		// Constraint is the parameter's constraint (`comparable`, `any`, `~int | ~string`, ...).
		// It is nil for self-referencing constraints (e.g. `T interface{ Less(T) bool }` seen
		// again while building T itself) to avoid infinite recursion.
		Constraint *TypeSpec `json:"constraint,omitempty" yaml:"constraint,omitempty"`
	}

	// Type represents a Go type discovered in the AST, including its specification,
	// name, and associated function, struct, or interface details if applicable.
	Type struct {
		*TypeNode `json:",inline" yaml:",inline"`
		Spec      TypeSpec   `json:"spec" yaml:"spec"`
		Name      string     `json:"name,omitempty" yaml:"name,omitempty"`
		Function  *Function  `json:"function,omitempty" yaml:"function,omitempty"`
		Struct    *Struct    `json:"struct,omitempty" yaml:"struct,omitempty"`
		Interface *Interface `json:"interface,omitempty" yaml:"interface,omitempty"`
	}

	// TypeDefinition represents a type definition in the AST, containing its specification.
	// It is used mostly for representing types of the arguments and results of functions,
	// where we don't want to include the full struct/interface details due to finite serialization depth.
	TypeDefinition struct {
		Spec TypeSpec `json:"spec" yaml:"spec"`
	}

	// Tag represents a struct field tag, including its name and value.
	Tag struct {
		Name  string `json:"name" yaml:"name"`
		Value string `json:"value" yaml:"value"`
	}

	// Var represents a variable, function argument, or struct field, including its name,
	// type, documentation, annotations, and tags (for struct fields).
	Var struct {
		Package      *Package        `json:"-" yaml:"-"`
		Name         string          `json:"name,omitempty" yaml:"name,omitempty"`
		FallbackName string          `json:"fallbackName,omitempty" yaml:"fallbackName,omitempty"`
		Doc          string          `json:"doc,omitempty" yaml:"doc,omitempty"`
		Embedded     bool            `json:"embedded,omitempty" yaml:"embedded,omitempty"`
		Annotations  Annotations     `json:"annotations,omitempty" yaml:"annotations,omitempty"`
		Type         *TypeDefinition `json:"type" yaml:"type"`
		Tags         []Tag
		Position
	}

	// Function represents a function or method, including its name, receiver (for methods),
	// arguments, results, and associated documentation and annotations.
	Function struct {
		TypeNode `json:",inline" yaml:",inline"`
		Name     string `json:"name" yaml:"name"`
		// TypeParams are the type parameters of a generic function (`func Map[T, R any](...)`).
		// Methods of generic types have none; their receiver type carries the parameters as
		// TypeArgs of kind KindTypeParam.
		TypeParams []*TypeParam `json:"typeParams,omitempty" yaml:"typeParams,omitempty"`
		Receiver   *Var         `json:"receiver,omitempty" yaml:"receiver,omitempty"`
		Args       []*Var       `json:"arguments,omitempty" yaml:"args,omitempty"`
		Results    []*Var       `json:"results,omitempty" yaml:"returns,omitempty"`
	}

	// Interface represents an interface type, including its underlying *types.Interface and its methods.
	Interface struct {
		Interface *types.Interface `json:"-" yaml:"-"`
		Methods   []*Function      `json:"methods,omitempty" yaml:"methods,omitempty"`
	}

	// Struct represents a struct type, including its underlying *types.Struct and its fields.
	Struct struct {
		Struct  *types.Struct `json:"-" yaml:"-"`
		Methods []*Function   `json:"methods,omitempty" yaml:"methods,omitempty"`
		Fields  []*Var        `json:"fields,omitempty" yaml:"fields,omitempty"`
	}
)

// Kind constants for TypeKind.Kind
const (
	// KindStruct represents a struct type.
	KindStruct Kind = "struct"
	// KindInterface represents an interface type.
	KindInterface Kind = "interface"
	// KindFunc represents a function type.
	KindFunc Kind = "func"
	// KindMap represents a map type.
	KindMap Kind = "map"
	// KindSlice represents a slice type.
	KindSlice Kind = "slice"
	// KindArray represents an array type.
	KindArray Kind = "array"
	// KindPointer represents a pointer type.
	KindPointer Kind = "pointer"
	// KindChan represents a channel type.
	KindChan Kind = "chan"
	// KindUnknown represents an unknown or unsupported type kind.
	KindUnknown Kind = "unknown"
	// KindString represents a string type.
	KindString Kind = "string"
	// KindInt represents an integer type.
	KindInt Kind = "int"
	// KindFloat represents a floating-point type.
	KindFloat Kind = "float"
	// KindBool represents a boolean type.
	KindBool Kind = "bool"
	// KindTypeParam represents a usage of a type parameter (e.g. `T` inside a generic type).
	KindTypeParam Kind = "typeparam"
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

// TypeByName searches all packages for a type matching the given unqualified name.
// If multiple types share the same name across packages, the first match is returned.
func (p Packages) TypeByName(name string) *Type {
	for _, pkg := range p {
		for _, t := range pkg.Types {
			if t.Name == name {
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

// GetPackage returns the package associated with the TypeNode.
// This method is part of the Node interface and allows access to the package information from any node in the AST.
func (p *Package) GetPackage() *Package {
	return p
}

// EnsureDir populates Dir when it is empty by inspecting the underlying decorator.Package.
// The first available file path in GoFiles, CompiledGoFiles, or the syntax/Fset positions
// is used to derive the absolute filesystem directory of the package. Returns the resulting
// Dir value (which is "" when no source-of-truth could be found).
func (p *Package) EnsureDir() string {
	if p == nil {
		return ""
	}
	if p.Dir != "" {
		return p.Dir
	}
	if p.Package == nil {
		return ""
	}
	if len(p.Package.GoFiles) > 0 {
		p.Dir = filepath.Dir(p.Package.GoFiles[0])
		return p.Dir
	}
	if len(p.Package.CompiledGoFiles) > 0 {
		p.Dir = filepath.Dir(p.Package.CompiledGoFiles[0])
		return p.Dir
	}
	if p.Package.Decorator != nil {
		for _, f := range p.Package.Syntax {
			if filename := p.Package.Decorator.Filenames[f]; filename != "" {
				p.Dir = filepath.Dir(filename)
				return p.Dir
			}
		}
	}
	return ""
}

// AnnotationOption is a functional option for configuring an Annotation.
type AnnotationOption func(*Annotation)

// WithNamedArgs returns an AnnotationOption that sets the named arguments on an Annotation.
func WithNamedArgs(namedArgs map[string]string) AnnotationOption {
	return func(a *Annotation) {
		a.NamedArgs = namedArgs
	}
}

// NamedArgsTemplateData converts named arguments into the map[string]any form
// exposed to templates as .Source.NamedArgs. The generic map type allows sprig
// dict functions such as dig to be used on it, e.g.
// {{ dig "file" "generated.go" .Source.NamedArgs }}. The result is never nil.
func NamedArgsTemplateData(namedArgs map[string]string) map[string]any {
	result := make(map[string]any, len(namedArgs))
	for k, v := range namedArgs {
		result[k] = v
	}
	return result
}

// WithImpliedBy returns an AnnotationOption that records the annotation that
// caused this annotation to be created (e.g., the @macro or plumber:mixin
// annotation it was expanded from). When implied is nil the option is a no-op.
func WithImpliedBy(implied Annotation) AnnotationOption {
	return func(a *Annotation) {
		a.ImpliedBy = &implied
	}
}

// WithOptionalImpliedBy returns an AnnotationOption that records the annotation that
// caused this annotation to be created (e.g., the @macro or plumber:mixin
// annotation it was expanded from). When implied is nil the option is a no-op.
func WithOptionalImpliedBy(implied *Annotation) AnnotationOption {
	return func(a *Annotation) {
		if implied != nil {
			a.ImpliedBy = implied
		}
	}
}

func WithSource(source Node) AnnotationOption {
	return func(a *Annotation) {
		a.Source = source
	}
}

func WithDocLine(docLine int) AnnotationOption {
	return func(a *Annotation) {
		a.DocLine = docLine
	}
}

func NewAnnotation(name string, args []string, opts ...AnnotationOption) Annotation {
	a := Annotation{Name: name, Args: args}
	for _, opt := range opts {
		opt(&a)
	}
	return a
}

func (aa Annotations) Find(name string) *Annotation {
	for _, ann := range aa {
		if ann.Name == name {
			return &ann
		}
	}
	return nil
}

func (aa Annotations) FindOr(name string, defaultValues ...string) *Annotation {
	for _, ann := range aa {
		if ann.Name == name {
			return &ann
		}
	}
	return &Annotation{Name: name, Args: defaultValues}
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

func (aa Annotations) FlatArgs() []string {
	return lo.FlatMap(aa, func(a Annotation, _ int) []string {
		return a.Args
	})
}

// TrimSingular removes earlier occurrences of annotations whose names are in singularNames,
// keeping only the last occurrence of each. Non-singular annotations are always preserved.
func (aa Annotations) TrimSingular(singularNames map[string]bool) Annotations {
	if len(singularNames) == 0 {
		return aa
	}
	// Find the last index of each singular annotation
	lastIndex := make(map[string]int, len(singularNames))
	for i, ann := range aa {
		if singularNames[ann.Name] {
			lastIndex[ann.Name] = i
		}
	}
	// Build result keeping only last occurrence of singular annotations
	result := make(Annotations, 0, len(aa))
	for i, ann := range aa {
		if singularNames[ann.Name] && i != lastIndex[ann.Name] {
			continue
		}
		result = append(result, ann)
	}
	return result
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

func (n *Var) GetDoc() string {
	return n.Doc
}

func (n *Var) GetPackage() *Package {
	return n.Package
}

func (n *TypeNode) GetAnnotations() Annotations {
	return n.Annotations
}

func (n *Var) GetAnnotations() Annotations {
	return n.Annotations
}

func (n *Var) GetPosition() Position {
	return n.Position
}

func (m *Annotation) Value() string {
	if len(m.Args) > 0 {
		return m.Args[0]
	}
	return ""
}

func (m *Annotation) SetValue(value string) {
	if len(m.Args) > 0 {
		m.Args[0] = value
	} else {
		m.Args = []string{value}
	}
}

func (m *Annotation) ValueOr(defaultValue string) string {
	if m == nil || len(m.Args) == 0 {
		return defaultValue
	}
	return m.Value()
}

func (n *CommentGroup) GetAnnotations() Annotations {
	return n.Annotations
}

func (n *CommentGroup) GetDoc() string {
	return n.Doc
}

func (n *CommentGroup) GetPosition() Position {
	return n.Position
}

func (n *CommentGroup) GetPackage() *Package {
	return n.Package
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
		Position:    n.Position,
		Package:     n.Package,
	}
}

func (t TypeKind) Indirect() *TypeKind {
	if t.Kind == "pointer" {
		return t.Elem
	}
	return &t
}

func buildTypeKind(_ *astx.FQN, t types.Type) TypeKind {
	if t == nil {
		return TypeKind{}
	}
	return newKindBuilder().kind(t)
}

// typeKindFromType maps a go/types.Type into a TypeKind tree. See kindBuilder.kind.
func typeKindFromType(t types.Type) TypeKind {
	return newKindBuilder().kind(t)
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

func basicTypeIs(b *types.Basic) Is {
	return Is{
		Boolean:  b.Info()&types.IsBoolean != 0,
		Integer:  b.Info()&types.IsInteger != 0,
		Unsigned: b.Info()&types.IsUnsigned != 0,
		Float:    b.Info()&types.IsFloat != 0,
		Complex:  b.Info()&types.IsComplex != 0,
		String:   b.Info()&types.IsString != 0,
		Untyped:  b.Info()&types.IsUntyped != 0,

		Ordered:   b.Info()&types.IsOrdered != 0,
		Numeric:   b.Info()&types.IsNumeric != 0,
		ConstType: b.Info()&types.IsConstType != 0,
	}
}

func (k Kind) String() string {
	return string(k)
}

func (p Position) String() string {
	return fmt.Sprintf("%s:%d at %d col", p.Filename, p.Line, p.Column)
}
