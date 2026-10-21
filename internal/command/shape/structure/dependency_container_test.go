// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for the dependency container lookup of structure paths.

package structure

import (
	"path/filepath"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
)

// structurePath creates a structure path config.
func structurePath(name, p, container string) config.StructurePathConfig {
	return config.StructurePathConfig{Path: config.PlumberStructurePathConfig{
		Name: name, Path: p, DependencyContainer: container,
	}}
}

func TestDependencyContainerLookup(t *testing.T) {
	moduleDir := t.TempDir()
	module := contract.ModuleInfo{Name: "svc", NormalizedName: "svc", Dir: moduleDir}
	defs := &config.StructureDefinitions{Structures: []config.PlumberStructureConfig{{
		Name: "hexagonal",
		Path: "internal/{{ .Module.NormalizedName }}",
		Paths: []config.StructurePathConfig{
			structurePath("adapter.outbound.pgsql", "adapter/outbound/pgsql", "database"),
			structurePath("adapter.outbound.mysql", "adapter/outbound/mysql", "database"),
			structurePath("adapter.outbound.redis", "adapter/outbound/redis", ""),
		},
	}}}

	lookup, err := NewDependencyContainerLookup(defs, module, module)
	assert.NilError(t, err)

	base := filepath.Join(moduleDir, "internal/svc/adapter/outbound")
	for _, dir := range []string{"pgsql", "mysql", "pgsql/"} {
		name, ok := lookup(filepath.Join(base, dir))
		assert.Assert(t, ok, dir)
		assert.Equal(t, name, "database")
	}

	_, ok := lookup(filepath.Join(base, "redis"))
	assert.Assert(t, !ok, "no dependency_container defined")
	_, ok = lookup(filepath.Join(base, "pgsql/sub"))
	assert.Assert(t, !ok, "only exact structure path directories match")

	// Definitions are not mutated by the expansion.
	assert.Equal(t, defs.Structures[0].Path, "internal/{{ .Module.NormalizedName }}")
}
