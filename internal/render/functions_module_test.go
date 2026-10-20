// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for module related template functions (module_path, module_import).

package render

import (
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/getoutreach/plumber/query/model"
)

// newModuleTestContext creates a render context for package example.com/app/svc with a
// resolver translating structure: paths.
func newModuleTestContext() *RenderContext {
	c := NewRenderContext(NewModuleRegister(), &model.Package{Path: "example.com/app/svc"}, "out.go")
	c.PathResolver = func(p string) (string, error) {
		if after, ok := strings.CutPrefix(p, "structure:"); ok {
			return "example.com/app/" + strings.ReplaceAll(after, ".", "/"), nil
		}
		return p, nil
	}
	return c
}

func TestModulePath(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"context", "context"},
		{"github.com/getoutreach/plumber", "github.com/getoutreach/plumber"},
		{"../domain", "example.com/app/domain"},
		{"./sub", "example.com/app/svc/sub"},
		{"structure:domain.entity", "example.com/app/domain/entity"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			c := newModuleTestContext()
			got, err := modulePath(c)(tc.in)
			assert.NilError(t, err)
			assert.Equal(t, got, tc.want)
			assert.Equal(t, len(c.Modules.Registrations()), 0, "module_path must not register modules")
		})
	}
}

func TestModulePathEmpty(t *testing.T) {
	_, err := modulePath(newModuleTestContext())("")
	assert.ErrorContains(t, err, "module path is required")
}

func TestModuleImportRegisters(t *testing.T) {
	c := newModuleTestContext()
	_, err := moduleImport(c)("structure:domain.entity")
	assert.NilError(t, err)
	_, ok := c.Modules.Registrations()["example.com/app/domain/entity"]
	assert.Assert(t, ok, "module_import must register the resolved module")
}
