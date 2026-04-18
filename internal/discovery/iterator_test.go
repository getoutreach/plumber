// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Tests for path iterator, template hydration, and derived variable expansion
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
			name:      "capitalize simple",
			template:  "{{ .module | capitalize }}",
			variables: map[string]string{"module": "async"},
			expected:  "Async",
		},
		{
			name:      "capitalize nested path",
			template:  "{{ .module | capitalize }}",
			variables: map[string]string{"module": "outbound/redis"},
			expected:  "OutboundRedis",
		},
		{
			name:      "capitalize deep nested path",
			template:  "{{ .module | capitalize }}",
			variables: map[string]string{"module": "adapter/inbound/redis"},
			expected:  "AdapterInboundRedis",
		},
		{
			name:      "upper function",
			template:  "{{ .module | upper }}",
			variables: map[string]string{"module": "async"},
			expected:  "ASYNC",
		},
		{
			name:      "slug simple",
			template:  "{{ .module | slug }}",
			variables: map[string]string{"module": "async"},
			expected:  "async",
		},
		{
			name:      "slug nested path",
			template:  "{{ .module | slug }}",
			variables: map[string]string{"module": "outbound/redis"},
			expected:  "outbound_redis",
		},
		{
			name:      "slug deep nested path",
			template:  "{{ .module | slug }}",
			variables: map[string]string{"module": "adapter/inbound/redis"},
			expected:  "adapter_inbound_redis",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := discovery.HydrateTemplate(tt.template, tt.variables)
			assert.Equal(t, result, tt.expected)
		})
	}
}

func TestCapitalize(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"async", "Async"},
		{"outbound/redis", "OutboundRedis"},
		{"adapter/inbound/redis", "AdapterInboundRedis"},
		{"single", "Single"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, discovery.Capitalize(tt.input), tt.expected)
		})
	}
}

func TestSlug(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"async", "async"},
		{"outbound/redis", "outbound_redis"},
		{"adapter/inbound/redis", "adapter_inbound_redis"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, discovery.Slug(tt.input), tt.expected)
		})
	}
}

func TestDeriveVariables(t *testing.T) {
	t.Run("simple value", func(t *testing.T) {
		raw := map[string]string{"module": "async"}
		derived := discovery.DeriveVariables(raw)

		assert.Equal(t, derived["module"], "Async")
		assert.Equal(t, derived["module_slug"], "async")
		assert.Equal(t, derived["module_path"], "async")
	})

	t.Run("nested path value", func(t *testing.T) {
		raw := map[string]string{"module": "outbound/redis"}
		derived := discovery.DeriveVariables(raw)

		assert.Equal(t, derived["module"], "OutboundRedis")
		assert.Equal(t, derived["module_slug"], "outbound_redis")
		assert.Equal(t, derived["module_path"], "outbound/redis")
	})

	t.Run("multiple variables", func(t *testing.T) {
		raw := map[string]string{"module": "outbound/redis", "prefix": "adapter"}
		derived := discovery.DeriveVariables(raw)

		assert.Equal(t, derived["module"], "OutboundRedis")
		assert.Equal(t, derived["module_slug"], "outbound_redis")
		assert.Equal(t, derived["module_path"], "outbound/redis")
		assert.Equal(t, derived["prefix"], "Adapter")
		assert.Equal(t, derived["prefix_slug"], "adapter")
		assert.Equal(t, derived["prefix_path"], "adapter")
	})
}

func TestPathIterator(t *testing.T) {
	// Create temporary directory structure
	tmpDir := t.TempDir()

	// Create test directories with Go files
	dirsWithGoFiles := []string{
		"adapter/async",
		"adapter/database",
		"adapter/grpc",
	}
	for _, dir := range dirsWithGoFiles {
		dirPath := filepath.Join(tmpDir, dir)
		err := os.MkdirAll(dirPath, 0o755)
		assert.NilError(t, err)
		// Create a .go file in each
		err = os.WriteFile(filepath.Join(dirPath, "main.go"), []byte("package "+filepath.Base(dir)), 0o600)
		assert.NilError(t, err)
	}

	// Create directories without Go files (should be skipped)
	dirsWithoutGoFiles := []string{
		"other/test",
		"adapter/empty",
	}
	for _, dir := range dirsWithoutGoFiles {
		err := os.MkdirAll(filepath.Join(tmpDir, dir), 0o755)
		assert.NilError(t, err)
	}

	// Test pattern matching
	pattern := `adapter/(?P<module>\w+)`
	iterator, err := discovery.NewPathIterator(tmpDir, pattern)
	assert.NilError(t, err)

	matches, err := iterator.Iterate()
	assert.NilError(t, err)
	assert.Equal(t, len(matches), 3, "should find 3 modules (adapter/empty has no .go files)")

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

func TestPathIteratorNestedDirectories(t *testing.T) {
	tmpDir := t.TempDir()

	// Create nested directory structure:
	// adapter/outbound/       (no .go files — intermediate, should be skipped)
	// adapter/outbound/redis/ (has .go files — should be matched)
	// adapter/async/          (has .go files — should be matched)
	dirs := map[string]bool{
		"adapter/outbound":       false, // no Go files
		"adapter/outbound/redis": true,  // has Go files
		"adapter/async":          true,  // has Go files
	}

	for dir, hasGo := range dirs {
		dirPath := filepath.Join(tmpDir, dir)
		err := os.MkdirAll(dirPath, 0o755)
		assert.NilError(t, err)
		if hasGo {
			err = os.WriteFile(filepath.Join(dirPath, "main.go"), []byte("package "+filepath.Base(dir)), 0o600)
			assert.NilError(t, err)
		}
	}

	// Use [\w/]+ to capture nested paths
	pattern := `adapter/(?P<module>[\w/]+)`
	iterator, err := discovery.NewPathIterator(tmpDir, pattern)
	assert.NilError(t, err)

	matches, err := iterator.Iterate()
	assert.NilError(t, err)

	// Collect matched module values
	modules := make(map[string]bool)
	for _, match := range matches {
		modules[match.Variables["module"]] = true
	}

	assert.Assert(t, modules["async"], "should find async")
	assert.Assert(t, modules["outbound/redis"], "should find outbound/redis")
	assert.Assert(t, !modules["outbound"], "should NOT find outbound (no .go files)")
	assert.Equal(t, len(matches), 2)
}

func TestHydrateConfig(t *testing.T) {
	cfg := &discovery.PlumberContainerConfig{
		Name:    "{{ .module }}",
		Comment: "Adapter for {{ .module_path }}",
		Container: discovery.ContainerPathConfig{
			Path: "./application_{{ .module_slug }}.go",
		},
		Source: &discovery.SourcePathConfig{
			Path: "./adapter/{{ .module_path }}/",
		},
	}

	matches := []discovery.PathMatch{
		{
			Path:      "/test/adapter/async",
			Variables: map[string]string{"module": "async"},
		},
		{
			Path:      "/test/adapter/outbound/redis",
			Variables: map[string]string{"module": "outbound/redis"},
		},
	}

	hydrated := discovery.HydrateConfig(cfg, matches)
	assert.Equal(t, len(hydrated), 2)

	// Check first hydrated config (simple module)
	assert.Equal(t, hydrated[0].Name, "Async")
	assert.Equal(t, hydrated[0].Comment, "Adapter for async")
	assert.Equal(t, hydrated[0].Container.Path, "./application_async.go")
	assert.Equal(t, hydrated[0].Source.Path, "./adapter/async/")

	// Check second hydrated config (nested module)
	assert.Equal(t, hydrated[1].Name, "OutboundRedis")
	assert.Equal(t, hydrated[1].Comment, "Adapter for outbound/redis")
	assert.Equal(t, hydrated[1].Container.Path, "./application_outbound_redis.go")
	assert.Equal(t, hydrated[1].Source.Path, "./adapter/outbound/redis/")
}

func TestNewPathIteratorInvalidPattern(t *testing.T) {
	_, err := discovery.NewPathIterator("/tmp", "[invalid")
	assert.ErrorContains(t, err, "invalid path pattern")
}
