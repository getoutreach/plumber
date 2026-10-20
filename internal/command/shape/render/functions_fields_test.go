// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for the fields_match template function.

package render

import (
	"bytes"
	"testing"
	"text/template"

	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
)

func vars(names ...string) []*model.Var {
	out := make([]*model.Var, 0, len(names))
	for _, n := range names {
		out = append(out, &model.Var{Name: n})
	}
	return out
}

func tagged(name string, tags ...model.Tag) *model.Var {
	return &model.Var{Name: name, Tags: tags}
}

func names(vs []*model.Var) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, v.Name)
	}
	return out
}

func TestFieldsMatch(t *testing.T) {
	tests := []struct {
		name   string
		a, b   []*model.Var
		method string
		want   []string
	}{
		{
			name: "strict matches identical names in order of a",
			a:    vars("ID", "Name", "Email"), b: vars("Email", "ID", "Other"),
			method: "strict", want: []string{"ID", "Email"},
		},
		{
			name: "empty method defaults to strict",
			a:    vars("ID", "userId"), b: vars("ID", "UserID"),
			method: "", want: []string{"ID"},
		},
		{
			name: "strict is case sensitive",
			a:    vars("UserID"), b: vars("UserId"),
			method: "strict", want: []string{},
		},
		{
			name: "tolerant normalizes case and separators",
			a:    vars("UserID", "user_name", "Email"), b: vars("userId", "UserName", "Phone"),
			method: "tolerant", want: []string{"UserID", "user_name"},
		},
		{
			name:   "tolerant matches a tag of a against name of b",
			a:      []*model.Var{tagged("Identifier", model.Tag{Name: "json", Value: "user_id,omitempty"})},
			b:      vars("UserID"),
			method: "tolerant", want: []string{"Identifier"},
		},
		{
			name:   "tolerant matches tags on both sides",
			a:      []*model.Var{tagged("A", model.Tag{Name: "db", Value: "created_at"})},
			b:      []*model.Var{tagged("B", model.Tag{Name: "json", Value: "createdAt"})},
			method: "tolerant", want: []string{"A"},
		},
		{
			name:   "tolerant ignores dash and empty tag names",
			a:      []*model.Var{tagged("A", model.Tag{Name: "json", Value: "-"}, model.Tag{Name: "db", Value: ",omitempty"})},
			b:      vars("-", ""),
			method: "tolerant", want: []string{},
		},
		{
			name: "method is case and space insensitive",
			a:    vars("user_id"), b: vars("UserID"),
			method: " Tolerant ", want: []string{"user_id"},
		},
		{
			name: "nil entries and nil slices are ignored",
			a:    []*model.Var{nil, {Name: "ID"}}, b: []*model.Var{nil, {Name: "ID"}},
			method: "strict", want: []string{"ID"},
		},
		{
			name: "nil b matches nothing",
			a:    vars("ID"), b: nil,
			method: "strict", want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fieldsMatch(tt.a, tt.b, tt.method)
			if err != nil {
				t.Fatalf("fieldsMatch() error = %v", err)
			}
			gotNames := names(got)
			if len(gotNames) != len(tt.want) {
				t.Fatalf("fieldsMatch() = %v, want %v", gotNames, tt.want)
			}
			for i := range tt.want {
				if gotNames[i] != tt.want[i] {
					t.Fatalf("fieldsMatch() = %v, want %v", gotNames, tt.want)
				}
			}
		})
	}
}

func TestFieldsMatch_UnknownMethod(t *testing.T) {
	if _, err := fieldsMatch(vars("ID"), vars("ID"), "fuzzy"); err == nil {
		t.Fatal("expected error for unknown method")
	}
}

// TestFieldsMatch_Described verifies the function is part of the documented catalog
// (plumber shape describe functions) and is callable from a template through it.
func TestFieldsMatch_Described(t *testing.T) {
	desc, build := FunctionsDescription()
	found := false
	for _, d := range desc.Descriptions() {
		if d.Name == "fields_match" {
			found = d.Description != "" && d.Usage != ""
		}
	}
	if !found {
		t.Fatal("fields_match must be described with a description and usage")
	}

	fm, dispose := build(&Context{
		ContextCloner: &render.RenderContext{Modules: render.NewModuleRegister()},
		Ignores:       NewIgnores(),
	}, nil, "")
	defer dispose()
	tpl := template.Must(template.New("t").Funcs(fm).Parse(
		`{{ range $f := fields_match .A .B "tolerant" }}{{ $f.Name }};{{ end }}`))

	var buf bytes.Buffer
	data := map[string]any{"A": vars("UserID", "Name"), "B": vars("user_id")}
	if err := tpl.Execute(&buf, data); err != nil {
		t.Fatalf("template execution failed: %v", err)
	}
	if buf.String() != "UserID;" {
		t.Fatalf("unexpected output %q", buf.String())
	}
}
