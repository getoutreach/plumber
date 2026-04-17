// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Tests for path iterator and template hydration
// Managed: true

package discovery_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/getoutreach/plumber/internal/discovery"
	"gotest.tools/v3/assert"
)

func TestHydrateTemplate(t *testing.T) {
	tests := []struct {
		name      string
		template  string
		variables map[string]string
		expected  string
	}{
		{
			name:      "single variable",
			template:  "{{ .module }}",
			variables: map[string]string{"module": "async"},
			expected:  "async",
		},
		{
			name:      "multiple variables",
			template:  "{{ .prefix }}_{{ .module }}",
			variables: map[string]string{"prefix": "app", "module": "database"},
			expected:  "app_database",
		},
		{
			name:      "no variables",
			template:  "constant",
			variables: map[string]string{},
			expected:  "constant",
		},
		{
			name:      "path with variables",
			template:  "./adapter/{{ .module }}/{{ .module }}.go",
			variables: map[string]string{"module": "grpc"},
			expected:  "./adapter/grpc/grpc.go",
		},
		{
			name:      "capitalize function",
			template:  "{{ .module | capitalize }}",
			variables: map[string]string{"module": "async"},
			expected:  "Async",
		},
		{
			name:      "upper function",
			template:  "{{ .module | upper }}",
			variables: map[string]string{"module": "async"},
			expected:  "ASYNC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := discovery.HydrateTemplate(tt.template, tt.variables)
			assert.Equal(t, result, tt.expected)
		})
	}
}

func TestPathIterator(t *testing.T) {
	// Create temporary directory structure
	tmpDir := t.TempDir()

	// Create test directories
	dirs := []string{
		"adapter/async",
		"adapter/database",
		"adapter/grpc",
		"other/test",
	}

	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(tmpDir, dir), 0o755)
		assert.NilError(t, err)
	}

	// Test pattern matching
	pattern := `adapter/(?P<module>\w+)`
	iterator, err := discovery.NewPathIterator(tmpDir, pattern)
	assert.NilError(t, err)

	matches, err := iterator.Iterate()
	assert.NilError(t, err)
	assert.Equal(t, len(matches), 3)

	// Check that we found the right modules
	modules := make(map[string]bool)
	for _, match := range matches {
		module, ok := match.Variables["module"]
		assert.Assert(t, ok, "module variable should be present")
		modules[module] = true
	}

	assert.Assert(t, modules["async"], "should find async module")
	assert.Assert(t, modules["database"], "should find database module")
	assert.Assert(t, modules["grpc"], "should find grpc module")
}

func TestHydrateConfig(t *testing.T) {
	cfg := &discovery.PlumberContainerConfig{
		Name:    "{{ .module }}",
		Comment: "Adapter for {{ .module }}",
		Container: discovery.ContainerPathConfig{
			Path: "./adapter/{{ .module }}/{{ .module }}.go",
		},
	}

	matches := []discovery.PathMatch{
		{
			Path:      "/test/adapter/async",
			Variables: map[string]string{"module": "async"},
		},
		{
			Path:      "/test/adapter/database",
			Variables: map[string]string{"module": "database"},
		},
	}

	hydrated := discovery.HydrateConfig(cfg, matches)
	assert.Equal(t, len(hydrated), 2)

	// Check first hydrated config
	assert.Equal(t, hydrated[0].Name, "async")
	assert.Equal(t, hydrated[0].Comment, "Adapter for async")
	assert.Equal(t, hydrated[0].Container.Path, "./adapter/async/async.go")

	// Check second hydrated config
	assert.Equal(t, hydrated[1].Name, "database")
	assert.Equal(t, hydrated[1].Comment, "Adapter for database")
	assert.Equal(t, hydrated[1].Container.Path, "./adapter/database/database.go")
}

func TestNewPathIteratorInvalidPattern(t *testing.T) {
	_, err := discovery.NewPathIterator("/tmp", "[invalid")
	assert.ErrorContains(t, err, "invalid path pattern")
}
