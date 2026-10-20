// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file builds TypeKind trees from go/types, including generic information
// (type parameters of declarations and functions, type arguments of instantiated types,
// type-parameter usages) and resolution of type aliases.

package model

import (
	"go/types"

	"github.com/getoutreach/plumber/internal/astx"
)

// kindBuilder converts go/types.Type values into TypeKind trees. It tracks the type
// parameters currently being built so that self-referencing constraints
// (`T interface{ Less(T) bool }`, `T Comparer[T]`) terminate.
type kindBuilder struct {
	inProgress map[*types.TypeParam]bool
}

func newKindBuilder() *kindBuilder {
	return &kindBuilder{inProgress: make(map[*types.TypeParam]bool)}
}

// kind recursively maps t into a TypeKind, filling Key/Elem for composite types,
// Underlaying for named types, and generic information where applicable.
func (b *kindBuilder) kind(t types.Type) TypeKind {
	switch v := t.(type) {
	case *types.Basic:
		return TypeKind{Kind: basicTypeKind(v), Is: basicTypeIs(v)}

	case *types.Alias:
		// `type A = B` (and generic aliases): describe the aliased type.
		return b.kind(types.Unalias(v))

	case *types.Named:
		// The underlying type of a named type is always an unnamed type, so
		// recursing here will never re-enter this case (no infinite loop).
		underlyingTK := b.kind(v.Underlying())
		tk := TypeKind{
			Kind:        underlyingTK.Kind,
			Key:         underlyingTK.Key,
			Elem:        underlyingTK.Elem,
			Underlaying: &underlyingTK,
		}
		if args := v.TypeArgs(); args != nil && args.Len() > 0 {
			tk.Origin = astx.FQNFromGoType(v.Origin()).String()
			tk.TypeArgs = make([]*TypeSpec, 0, args.Len())
			for i := 0; i < args.Len(); i++ {
				spec := b.spec(args.At(i))
				tk.TypeArgs = append(tk.TypeArgs, &spec)
			}
		} else if params := v.TypeParams(); params != nil && params.Len() > 0 {
			tk.TypeParams = b.params(params)
		}
		return tk

	case *types.TypeParam:
		return TypeKind{Kind: KindTypeParam, Param: b.param(v)}

	case *types.Pointer:
		elem := b.kind(v.Elem())
		return TypeKind{Kind: KindPointer, Elem: &elem}

	case *types.Slice:
		elem := b.kind(v.Elem())
		return TypeKind{Kind: KindSlice, Elem: &elem}

	case *types.Array:
		elem := b.kind(v.Elem())
		return TypeKind{Kind: KindArray, Elem: &elem}

	case *types.Map:
		key := b.kind(v.Key())
		elem := b.kind(v.Elem())
		return TypeKind{Kind: KindMap, Key: &key, Elem: &elem}

	case *types.Chan:
		elem := b.kind(v.Elem())
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

// spec builds a TypeSpec (FQN + TypeKind) for t sharing the builder's recursion guard.
func (b *kindBuilder) spec(t types.Type) TypeSpec {
	return TypeSpec{
		TypeKind: b.kind(t),
		FQN:      astx.FQNFromGoType(t).String(),
		Type:     t,
	}
}

// params builds TypeParams for a type parameter list.
func (b *kindBuilder) params(list *types.TypeParamList) []*TypeParam {
	if list == nil || list.Len() == 0 {
		return nil
	}
	out := make([]*TypeParam, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		out = append(out, b.param(list.At(i)))
	}
	return out
}

// param builds a TypeParam. The constraint is omitted when the parameter is already
// being built higher up the tree (self-referencing constraint).
func (b *kindBuilder) param(tp *types.TypeParam) *TypeParam {
	p := &TypeParam{Name: tp.Obj().Name(), Index: tp.Index()}
	if b.inProgress[tp] {
		return p
	}
	b.inProgress[tp] = true
	defer delete(b.inProgress, tp)
	if c := tp.Constraint(); c != nil {
		spec := b.spec(c)
		p.Constraint = &spec
	}
	return p
}

// NewTypeParams converts a go/types type parameter list (e.g. from
// types.Signature.TypeParams or types.Named.TypeParams) into model TypeParams.
// It returns nil for a nil or empty list.
func NewTypeParams(list *types.TypeParamList) []*TypeParam {
	return newKindBuilder().params(list)
}

// IsGeneric reports whether the type is a generic declaration or an instantiation of one.
func (t TypeKind) IsGeneric() bool {
	return len(t.TypeParams) > 0 || len(t.TypeArgs) > 0
}

// IsInstantiated reports whether the type is an instantiation of a generic type
// (e.g. `Schema[string, int]`).
func (t TypeKind) IsInstantiated() bool {
	return len(t.TypeArgs) > 0
}

// IsTypeParam reports whether the type is a usage of a type parameter.
func (t TypeKind) IsTypeParam() bool {
	return t.Kind == KindTypeParam
}
