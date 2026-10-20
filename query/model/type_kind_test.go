// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for building TypeKind generic information and alias resolution.

package model

import (
	"encoding/json"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"testing"

	"github.com/getoutreach/plumber/internal/astx"
)

const genericSrc = `package gen

type Item struct{}

type Schema[K comparable, V any] struct {
	Key  K
	Vals []V
}

var Instance Schema[string, *Item]

var Nested Schema[int, Schema[string, bool]]

type Cmp[T any] interface{ Cmp(T) int }

type Sorted[T Cmp[T]] struct{ v T }

type Alias = []int

type GenericAlias[T any] = Schema[string, T]

var ViaAlias GenericAlias[int]

func Map[T, R any](in []T, f func(T) R) []R { return nil }
`

func checkGeneric(t *testing.T) *types.Package {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "gen.go", genericSrc, 0)
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := (&types.Config{Importer: importer.Default()}).Check("example.com/gen", fset, []*ast.File{f}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}

func specOf(t *testing.T, pkg *types.Package, name string) TypeSpec {
	t.Helper()
	obj := pkg.Scope().Lookup(name)
	if obj == nil {
		t.Fatalf("%s not found", name)
	}
	return NewTypeSpec(astx.FQNFromGoType(obj.Type()), obj.Type())
}

func TestTypeKind_GenericDeclaration(t *testing.T) {
	spec := specOf(t, checkGeneric(t), "Schema")

	if spec.Kind != KindStruct || !spec.IsGeneric() || spec.IsInstantiated() {
		t.Fatalf("unexpected kind/flags: %+v", spec.TypeKind)
	}
	if len(spec.TypeParams) != 2 {
		t.Fatalf("expected 2 type params, got %d", len(spec.TypeParams))
	}
	k, v := spec.TypeParams[0], spec.TypeParams[1]
	if k.Name != "K" || k.Index != 0 || v.Name != "V" || v.Index != 1 {
		t.Fatalf("unexpected params: %+v %+v", k, v)
	}
	if k.Constraint == nil || k.Constraint.FQN != "comparable" || k.Constraint.Kind != KindInterface {
		t.Fatalf("unexpected K constraint: %+v", k.Constraint)
	}
	if v.Constraint == nil || v.Constraint.FQN != "any" || v.Constraint.Kind != KindInterface {
		t.Fatalf("unexpected V constraint: %+v", v.Constraint)
	}
}

func TestTypeKind_TypeParamUsage(t *testing.T) {
	pkg := checkGeneric(t)
	st := pkg.Scope().Lookup("Schema").Type().Underlying().(*types.Struct)

	key := NewTypeSpec(astx.FQNFromGoType(st.Field(0).Type()), st.Field(0).Type())
	if !key.IsTypeParam() || key.Param == nil || key.Param.Name != "K" || key.FQN != "K" {
		t.Fatalf("unexpected Key field spec: %+v", key.TypeKind)
	}
	if key.Param.Constraint == nil || key.Param.Constraint.FQN != "comparable" {
		t.Fatalf("expected comparable constraint, got %+v", key.Param.Constraint)
	}

	vals := NewTypeSpec(astx.FQNFromGoType(st.Field(1).Type()), st.Field(1).Type())
	if vals.Kind != KindSlice || vals.Elem == nil || vals.Elem.Kind != KindTypeParam || vals.Elem.Param.Name != "V" {
		t.Fatalf("unexpected Vals field spec: %+v", vals.TypeKind)
	}
}

func TestTypeKind_Instantiated(t *testing.T) {
	spec := specOf(t, checkGeneric(t), "Instance")

	if spec.Kind != KindStruct || !spec.IsInstantiated() || len(spec.TypeParams) != 0 {
		t.Fatalf("unexpected kind/flags: %+v", spec.TypeKind)
	}
	if spec.Origin != `"example.com/gen".Schema` {
		t.Fatalf("unexpected origin %q", spec.Origin)
	}
	if len(spec.TypeArgs) != 2 {
		t.Fatalf("expected 2 type args, got %d", len(spec.TypeArgs))
	}
	if a := spec.TypeArgs[0]; a.FQN != "string" || a.Kind != KindString || !a.Is.String {
		t.Fatalf("unexpected arg 0: %+v", a)
	}
	if a := spec.TypeArgs[1]; a.FQN != `*"example.com/gen".Item` || a.Kind != KindPointer || a.Elem.Kind != KindStruct {
		t.Fatalf("unexpected arg 1: %+v", a)
	}
}

func TestTypeKind_NestedInstantiation(t *testing.T) {
	spec := specOf(t, checkGeneric(t), "Nested")
	inner := spec.TypeArgs[1]
	if !inner.IsInstantiated() || inner.Origin != `"example.com/gen".Schema` || inner.TypeArgs[1].Kind != KindBool {
		t.Fatalf("unexpected nested arg: %+v", inner.TypeKind)
	}
}

// TestTypeKind_SelfReferencingConstraint verifies `type Sorted[T Cmp[T]]` terminates and
// the inner reference to T carries no constraint.
func TestTypeKind_SelfReferencingConstraint(t *testing.T) {
	spec := specOf(t, checkGeneric(t), "Sorted")
	p := spec.TypeParams[0]
	if p.Name != "T" || p.Constraint == nil || p.Constraint.Origin != `"example.com/gen".Cmp` {
		t.Fatalf("unexpected T: %+v", p)
	}
	inner := p.Constraint.TypeArgs[0]
	if !inner.IsTypeParam() || inner.Param.Name != "T" || inner.Param.Constraint != nil {
		t.Fatalf("expected recursion to stop at T, got %+v", inner.TypeKind)
	}
	if _, err := json.Marshal(spec); err != nil {
		t.Fatalf("spec must be serializable: %v", err)
	}
}

func TestTypeKind_Alias(t *testing.T) {
	pkg := checkGeneric(t)

	alias := specOf(t, pkg, "Alias")
	if alias.Kind != KindSlice || alias.Elem.Kind != KindInt {
		t.Fatalf("unexpected alias kind: %+v", alias.TypeKind)
	}

	via := specOf(t, pkg, "ViaAlias")
	if via.Kind != KindStruct || !via.IsInstantiated() || via.TypeArgs[1].Kind != KindInt {
		t.Fatalf("unexpected generic alias kind: %+v", via.TypeKind)
	}
}

func TestNewTypeParams_GenericFunc(t *testing.T) {
	sig := checkGeneric(t).Scope().Lookup("Map").Type().(*types.Signature)
	params := NewTypeParams(sig.TypeParams())
	if len(params) != 2 || params[0].Name != "T" || params[1].Name != "R" || params[1].Index != 1 {
		t.Fatalf("unexpected params: %+v", params)
	}
	if params[0].Constraint == nil || params[0].Constraint.FQN != "any" {
		t.Fatalf("unexpected constraint: %+v", params[0].Constraint)
	}
	if NewTypeParams(nil) != nil {
		t.Fatal("expected nil for nil list")
	}
}
