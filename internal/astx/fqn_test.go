// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains tests for FQN conversion, parsing, and round-trip correctness.

package astx

import (
	"go/types"
	"testing"

	"gotest.tools/v3/assert"
)

// helpers to build types.Type values without loading real packages

var (
	pkgUUID = types.NewPackage("github.com/google/uuid", "uuid")
	pkgHTTP = types.NewPackage("net/http", "http")
	pkgFoo  = types.NewPackage("github.com/example/foo", "foo")

	typeUUID = namedType(pkgUUID, "UUID", types.Typ[types.Uint8])
	typeDir  = namedType(pkgHTTP, "Dir", types.Typ[types.String])
	typeBar  = namedType(pkgFoo, "Bar", types.Typ[types.Int])
)

func namedType(pkg *types.Package, name string, underlying types.Type) *types.Named {
	obj := types.NewTypeName(0, pkg, name, nil)
	return types.NewNamed(obj, underlying, nil)
}

func TestFQNFromGoType(t *testing.T) {
	tests := []struct {
		name     string
		typ      types.Type
		expected string
	}{
		{
			name:     "basic int",
			typ:      types.Typ[types.Int],
			expected: "int",
		},
		{
			name:     "basic string",
			typ:      types.Typ[types.String],
			expected: "string",
		},
		{
			name:     "named type with package",
			typ:      typeUUID,
			expected: `"github.com/google/uuid".UUID`,
		},
		{
			name:     "pointer to named type",
			typ:      types.NewPointer(typeUUID),
			expected: `*"github.com/google/uuid".UUID`,
		},
		{
			name:     "slice of named type",
			typ:      types.NewSlice(typeDir),
			expected: `[]"net/http".Dir`,
		},
		{
			name:     "pointer to slice of named type",
			typ:      types.NewPointer(types.NewSlice(typeDir)),
			expected: `*[]"net/http".Dir`,
		},
		{
			name:     "array of basic type",
			typ:      types.NewArray(types.Typ[types.Byte], 16),
			expected: `[16]uint8`,
		},
		{
			name:     "map of named types",
			typ:      types.NewMap(typeBar, typeUUID),
			expected: `map["github.com/example/foo".Bar]"github.com/google/uuid".UUID`,
		},
		{
			name:     "pointer to basic type",
			typ:      types.NewPointer(types.Typ[types.Bool]),
			expected: `*bool`,
		},
		{
			name:     "slice of basic type",
			typ:      types.NewSlice(types.Typ[types.String]),
			expected: `[]string`,
		},
		{
			name:     "bidirectional chan",
			typ:      types.NewChan(types.SendRecv, types.Typ[types.Int]),
			expected: `chan int`,
		},
		{
			name:     "send-only chan",
			typ:      types.NewChan(types.SendOnly, types.Typ[types.Int]),
			expected: `chan<- int`,
		},
		{
			name:     "recv-only chan",
			typ:      types.NewChan(types.RecvOnly, types.Typ[types.Int]),
			expected: `<-chan int`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FQNFromGoType(tt.typ).String()
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestParseFQN(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"basic int", "int"},
		{"basic string", "string"},
		{"basic bool", "bool"},
		{"named type with package", `"github.com/google/uuid".UUID`},
		{"pointer to named type", `*"github.com/google/uuid".UUID`},
		{"slice of named type", `[]"net/http".Dir`},
		{"pointer to slice of named type", `*[]"net/http".Dir`},
		{"array of basic type", `[16]uint8`},
		{"map of named types", `map["github.com/example/foo".Bar]"github.com/google/uuid".UUID`},
		{"map of basic types", `map[string]int`},
		{"nested map", `map[string]map[string]int`},
		{"pointer to basic type", `*bool`},
		{"slice of basic type", `[]string`},
		{"bidirectional chan", `chan int`},
		{"send-only chan", `chan<- int`},
		{"recv-only chan", `<-chan int`},
		{"generic type", `"github.com/getoutreach/plumber/example/contract".Filtrable["github.com/getoutreach/plumber/example/contract".Name]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fqn, err := ParseFQN(tt.input)
			assert.NilError(t, err)
			assert.Equal(t, tt.input, fqn.String())
		})
	}
}

func TestCraftFQN(t *testing.T) {
	tests := []struct {
		name   string
		pkg    string
		tp     string
		output string
	}{
		{"plain named type", "github.com/google/uuid", "UUID", `"github.com/google/uuid".UUID`},
		{"pointer to named type", "github.com/google/uuid", "*UUID", `*"github.com/google/uuid".UUID`},
		{"slice of named type", "net/http", "[]Dir", `[]"net/http".Dir`},
		{"pointer to slice of named type", "net/http", "*[]Dir", `*[]"net/http".Dir`},
		{"map of named types", "github.com/example/foo", "map[string]Bar", `map[string]"github.com/example/foo".Bar`},
		{"chan of named type", "github.com/google/uuid", "chan UUID", `chan "github.com/google/uuid".UUID`},
		{"send chan of named type", "github.com/google/uuid", "chan<- UUID", `chan<- "github.com/google/uuid".UUID`},
		{"recv chan of named type", "github.com/google/uuid", "<-chan UUID", `<-chan "github.com/google/uuid".UUID`},
		{"basic type with no pkg", "", "int", `int`},
		{"pointer to basic type with no pkg", "", "*string", `*string`},
		{"already qualified type", "", `*"github.com/google/uuid".UUID`, `*"github.com/google/uuid".UUID`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fqn, err := CraftFQN(tt.pkg, tt.tp)
			assert.NilError(t, err)
			assert.Equal(t, tt.output, fqn.String())
		})
	}
}

func TestParseFQNRoundTrip(t *testing.T) {
	testTypes := []struct {
		name string
		typ  types.Type
	}{
		{"basic int", types.Typ[types.Int]},
		{"named uuid", typeUUID},
		{"pointer to named", types.NewPointer(typeUUID)},
		{"slice of named", types.NewSlice(typeDir)},
		{"pointer to slice", types.NewPointer(types.NewSlice(typeDir))},
		{"array", types.NewArray(types.Typ[types.Byte], 16)},
		{"map", types.NewMap(typeBar, typeUUID)},
		{"send chan", types.NewChan(types.SendOnly, types.Typ[types.Int])},
		{"recv chan", types.NewChan(types.RecvOnly, types.Typ[types.Int])},
	}

	for _, tt := range testTypes {
		t.Run(tt.name, func(t *testing.T) {
			original := FQNFromGoType(tt.typ).String()
			parsed, err := ParseFQN(original)
			assert.NilError(t, err)
			assert.Equal(t, original, parsed.String())
		})
	}
}

func TestParseFQNError(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"lone star", "*"},
		{"unterminated pkg path", `"github.com/foo`},
		{"pkg path without type name", `"github.com/foo".`},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseFQN(tt.input)
			assert.ErrorContains(t, err, "")
		})
	}
}

func TestFQNWalkPackages(t *testing.T) {
	t.Run("replaces named type", func(t *testing.T) {
		fqn := FQNFromGoType(typeUUID)
		// "github.com/google/uuid".UUID  →  MyUUID
		fqn.WalkPackages(func(pkgPath, typeName string) (string, bool) {
			assert.Equal(t, "github.com/google/uuid", pkgPath)
			assert.Equal(t, "UUID", typeName)
			return "MyUUID", true
		})
		assert.Equal(t, "MyUUID.UUID", fqn.String())
	})

	t.Run("replaces named type inside pointer", func(t *testing.T) {
		fqn := FQNFromGoType(types.NewPointer(typeUUID))
		fqn.WalkPackages(func(_, _ string) (string, bool) { return "X", true })
		assert.Equal(t, "*X.UUID", fqn.String())
	})

	t.Run("replaces named type inside slice", func(t *testing.T) {
		fqn := FQNFromGoType(types.NewSlice(typeDir))
		fqn.WalkPackages(func(pkgPath, typeName string) (string, bool) {
			assert.Equal(t, "net/http", pkgPath)
			assert.Equal(t, "Dir", typeName)
			return "MyDir", true
		})
		assert.Equal(t, "[]MyDir.Dir", fqn.String())
	})

	t.Run("replaces both key and value in map", func(t *testing.T) {
		fqn := FQNFromGoType(types.NewMap(typeBar, typeUUID))
		calls := map[string]string{}
		fqn.WalkPackages(func(pkgPath, typeName string) (string, bool) {
			calls[pkgPath] = typeName
			return "", true
		})
		assert.Equal(t, 2, len(calls))
		assert.Equal(t, "map[Bar]UUID", fqn.String())
	})

	t.Run("replaces named type inside chan", func(t *testing.T) {
		fqn := FQNFromGoType(types.NewChan(types.SendOnly, typeUUID))
		fqn.WalkPackages(func(_, _ string) (string, bool) { return "T", true })
		assert.Equal(t, "chan<- T.UUID", fqn.String())
	})

	t.Run("nil return leaves node unchanged", func(t *testing.T) {
		fqn := FQNFromGoType(typeUUID)
		original := fqn.String()
		fqn.WalkPackages(func(_, _ string) (string, bool) { return "", false })
		assert.Equal(t, original, fqn.String())
	})

	t.Run("skips basic type with no remote package", func(t *testing.T) {
		fqn := FQNFromGoType(types.Typ[types.Int])
		called := false
		fqn.WalkPackages(func(_, _ string) (string, bool) { called = true; return "", false })
		assert.Equal(t, false, called)
		assert.Equal(t, "int", fqn.String())
	})
}
