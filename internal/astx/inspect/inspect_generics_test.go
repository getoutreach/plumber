// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests that Inspect populates generic information (type params, type args,
// type-parameter usages) on types, struct fields, functions and package variables.

package inspect

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

func TestInspect_Generics(t *testing.T) {
	dir, err := filepath.Abs(filepath.Join("testdata", "generics"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll("testdata") })

	src := `package generics

type Schema[K comparable, V any] struct {
	Key  K
	Vals []V
}

var UserSchema = Schema[string, int]{}

func Map[T, R any](in []T, f func(T) R) []R { return nil }

func (s *Schema[K, V]) Get(k K) V { var v V; return v }
`
	file := filepath.Join(dir, "generics.go")
	if err := os.WriteFile(file, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	pkgs, err := Inspect([]string{file}, dir)
	if err != nil {
		t.Fatalf("Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	schema, ok := lo.Find(pkg.Types, func(tp *model.Type) bool { return tp.Name == "Schema" })
	if !ok {
		t.Fatal("Schema not found")
	}
	if got := len(schema.Spec.TypeParams); got != 2 {
		t.Fatalf("expected 2 type params on Schema, got %d", got)
	}
	key := schema.Struct.Fields[0].Type.Spec
	if !key.IsTypeParam() || key.Param.Name != "K" {
		t.Fatalf("expected Key field to be type param K, got %+v", key.TypeKind)
	}

	get, ok := lo.Find(schema.Struct.Methods, func(f *model.Function) bool { return f.Name == "Get" })
	if !ok {
		t.Fatal("Get method not found")
	}
	recv := get.Receiver.Type.Spec
	if recv.Kind != model.KindPointer || len(recv.Elem.TypeArgs) != 2 || !recv.Elem.TypeArgs[0].IsTypeParam() {
		t.Fatalf("expected receiver *Schema[K, V] with type-param args, got %+v", recv.TypeKind)
	}

	mapFn, ok := lo.Find(pkg.Functions, func(f *model.Function) bool { return f.Name == "Map" })
	if !ok {
		t.Fatal("Map not found")
	}
	if len(mapFn.TypeParams) != 2 || mapFn.TypeParams[0].Name != "T" || mapFn.TypeParams[1].Name != "R" {
		t.Fatalf("unexpected Map type params: %+v", mapFn.TypeParams)
	}

	v, ok := lo.Find(pkg.Vars, func(v *model.PackageVar) bool { return v.Name == "UserSchema" })
	if !ok {
		t.Fatal("UserSchema not found")
	}
	if !v.Type.Spec.IsInstantiated() || v.Type.Spec.TypeArgs[0].FQN != "string" || v.Type.Spec.TypeArgs[1].Kind != model.KindInt {
		t.Fatalf("unexpected UserSchema spec: %+v", v.Type.Spec.TypeKind)
	}
}
