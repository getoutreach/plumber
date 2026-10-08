// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for blending generated variable initializers into existing package-level
// variables during inplace merge (e.g. new arguments of a schema constructor call).

package shape

import (
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/getoutreach/plumber/internal/astx/inspect"
	"github.com/getoutreach/plumber/query/model"
)

// schemaFixture lays out a package mirroring the reported use case:
//
//	var userSchema *mapping.Schema[entity.FieldName, table.Column] = mapping.NewSchema(
//		mapping.Column(mapping.EntityColumn(entity.Field_ID), table.Col(table.User, table.User.ID)),
//	)
//
// with mapping/entity/table as real sub-packages so selectors resolve to Ident.Path.
// schemaSrc renders schema/schema.go given the fixture's base import path. Returns the
// inspected schema package, its directory and the base import path.
func schemaFixture(t *testing.T, name string, schemaSrc func(base string) string) (*model.Package, string, string) {
	t.Helper()
	root := mergeTestFixtureDir(t, name)
	base := "github.com/getoutreach/plumber/internal/command/shape/testdata/" + name

	write := func(rel, src string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("entity/entity.go", `package entity

type FieldName string

const (
	Field_ID    FieldName = "id"
	Field_Name  FieldName = "name"
	Field_Email FieldName = "email"
)
`)
	write("table/table.go", `package table

type Column string

type userTable struct{ ID, Name, Email, Extra Column }

var User = userTable{ID: "id", Name: "name", Email: "email", Extra: "extra"}

func Col(_ userTable, c Column) Column { return c }
`)
	write("mapping/mapping.go", `package mapping

type Schema[K comparable, V any] struct{ cols []Mapping[K, V] }

type Mapping[K comparable, V any] struct {
	Key K
	Val V
}

type Key[K comparable] struct{ K K }

func EntityColumn[K comparable](k K) Key[K] { return Key[K]{K: k} }

func Column[K comparable, V any](k Key[K], v V) Mapping[K, V] { return Mapping[K, V]{Key: k.K, Val: v} }

func NewSchema[K comparable, V any](cols ...Mapping[K, V]) *Schema[K, V] { return &Schema[K, V]{cols: cols} }
`)
	write("schema/schema.go", schemaSrc(base))

	dir := filepath.Join(root, "schema")
	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "schema.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]
	if len(pkg.Package.Errors) != 0 {
		t.Fatalf("fixture does not compile: %v", pkg.Package.Errors)
	}
	return pkg, dir, base
}

// schemaSource renders a schema.go file with the given NewSchema arguments.
func schemaSource(base string, args ...string) string {
	return `package schema

import (
	"` + base + `/entity"
	"` + base + `/mapping"
	"` + base + `/table"
)

var userSchema *mapping.Schema[entity.FieldName, table.Column] = mapping.NewSchema(
` + strings.Join(args, "\n") + `
)
`
}

const (
	colID = `	// ID is a mapping for ID field.
	mapping.Column(
		mapping.EntityColumn(entity.Field_ID),
		table.Col(table.User, table.User.ID),
	),`
	colName = `	// Name is a mapping for Name field.
	mapping.Column(
		mapping.EntityColumn(entity.Field_Name),
		table.Col(table.User, table.User.Name),
	),`
	colEmail = `	// Email is a mapping for Email field.
	mapping.Column(
		mapping.EntityColumn(entity.Field_Email),
		table.Col(table.User, table.User.Email),
	),`
)

// mergeSchemaRun merges generated schema source into pkg, restores touched files to
// disk and returns the rendered schema.go (or "" when nothing was merged).
func mergeSchemaRun(t *testing.T, pkg *model.Package, dir, generated string) string {
	t.Helper()
	files, err := Merge(pkg, parseGeneratedLikeInplace(t, pkg, generated), "schema.go")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(files) == 0 {
		return ""
	}
	return restoreFiles(t, pkg, dir, files)["schema.go"]
}

// reinspect loads schema.go from disk again and fails when it does not compile.
func reinspect(t *testing.T, dir string) *model.Package {
	t.Helper()
	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "schema.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	if len(pkgs[0].Package.Errors) != 0 {
		t.Fatalf("merged package does not compile: %v", pkgs[0].Package.Errors)
	}
	return pkgs[0]
}

// assertOrder fails unless every needle occurs in haystack exactly once and in order.
func assertOrder(t *testing.T, haystack string, needles ...string) {
	t.Helper()
	pos := -1
	for _, n := range needles {
		if c := strings.Count(haystack, n); c != 1 {
			t.Fatalf("expected %q exactly once, found %d times in:\n%s", n, c, haystack)
		}
		i := strings.Index(haystack, n)
		if i < pos {
			t.Fatalf("expected %q after previous entries in:\n%s", n, haystack)
		}
		pos = i
	}
}

// TestMergeVarValue_AddsNewArgument covers the reported scenario: the template gains a
// new column and it must be blended into the existing NewSchema call.
func TestMergeVarValue_AddsNewArgument(t *testing.T) {
	pkg, dir, base := schemaFixture(t, "merge_var_value_add", func(b string) string { return schemaSource(b, colID) })

	generated := schemaSource(base, colID, colName)
	out := mergeSchemaRun(t, pkg, dir, generated)
	if out == "" {
		t.Fatal("expected schema.go to be merged")
	}
	assertOrder(t, out,
		"// ID is a mapping for ID field.", "entity.Field_ID",
		"// Name is a mapping for Name field.", "entity.Field_Name",
	)
	if c := strings.Count(out, "var userSchema"); c != 1 {
		t.Fatalf("expected a single userSchema declaration, got %d:\n%s", c, out)
	}

	// Second run with the same template: compiles and is a no-op.
	pkg = reinspect(t, dir)
	if out := mergeSchemaRun(t, pkg, dir, generated); out != "" {
		t.Fatalf("expected second run to be a no-op, got:\n%s", out)
	}
}

// TestMergeVarValue_InsertsInTemplateOrder verifies a missing argument is inserted after
// its preceding template argument rather than appended at the end.
func TestMergeVarValue_InsertsInTemplateOrder(t *testing.T) {
	pkg, dir, base := schemaFixture(t, "merge_var_value_order", func(b string) string {
		return schemaSource(b, colID, colEmail)
	})

	out := mergeSchemaRun(t, pkg, dir, schemaSource(base, colID, colName, colEmail))
	assertOrder(t, out, "entity.Field_ID", "entity.Field_Name", "entity.Field_Email")
	reinspect(t, dir)
}

// TestMergeVarValue_PreservesManualEdits verifies hand-written arguments are kept and an
// existing column whose trailing argument was edited is recognized (same target and
// first argument) instead of being added a second time.
func TestMergeVarValue_PreservesManualEdits(t *testing.T) {
	editedID := `	// ID is mapped to a custom column on purpose.
	mapping.Column(
		mapping.EntityColumn(entity.Field_ID),
		table.Col(table.User, table.User.Extra),
	),`
	manual := `	mapping.Column(mapping.EntityColumn(entity.FieldName("manual")), table.Column("manual")),`

	pkg, dir, base := schemaFixture(t, "merge_var_value_manual", func(b string) string {
		return schemaSource(b, editedID, manual)
	})

	out := mergeSchemaRun(t, pkg, dir, schemaSource(base, colID, colName))
	assertOrder(t, out,
		"entity.Field_ID", "table.User.Extra",
		"entity.Field_Name",
		`entity.FieldName("manual")`,
	)
	if strings.Contains(out, "table.User.ID") {
		t.Fatalf("edited ID column must not be duplicated or rewritten:\n%s", out)
	}
	// Doc follows the preserve-manual rule used across inplace merge: the manual
	// comment stays first and the generated line is appended.
	assertOrder(t, out, "custom column on purpose", "// ID is a mapping for ID field.")
	reinspect(t, dir)
}

// TestMergeVarValue_DifferentInitializerUntouched verifies an initializer that calls a
// different function than the template is left as written.
func TestMergeVarValue_DifferentInitializerUntouched(t *testing.T) {
	pkg, dir, base := schemaFixture(t, "merge_var_value_other", func(b string) string {
		return `package schema

import (
	"` + b + `/entity"
	"` + b + `/mapping"
	"` + b + `/table"
)

func buildSchema() *mapping.Schema[entity.FieldName, table.Column] { return nil }

var userSchema *mapping.Schema[entity.FieldName, table.Column] = buildSchema()
`
	})

	if out := mergeSchemaRun(t, pkg, dir, schemaSource(base, colID)); out != "" {
		t.Fatalf("expected no merge for a different initializer, got:\n%s", out)
	}
}

// TestMergeExprList_SliceLiteral verifies unkeyed composite literal elements are blended
// the same way as call arguments.
func TestMergeExprList_SliceLiteral(t *testing.T) {
	pkg, _ := inspectVarFixture(t, "merge_var_value_slice", map[string]string{
		"vars.go": "package merge_var_value_slice\n\nvar names = []string{\n\t\"a\",\n\t\"manual\",\n}\n",
	})
	generated := parseFile(t, "package merge_var_value_slice\n\nvar names = []string{\n\t\"a\",\n\t\"b\",\n}\n")
	files, err := Merge(pkg, generated, "vars.go")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 merged file, got %d", len(files))
	}
	got := exprSig(findTopLevelValueSpec(pkg, token.VAR, "names").Spec.Values[0])
	if got != `[]string{"a","b","manual"}` {
		t.Fatalf("unexpected merged literal: %s", got)
	}
}
