// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for the "container" loop variable and merging of loop containers sharing a dependency container.

package discovery_test

import (
	"os"
	"path/filepath"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/getoutreach/plumber/internal/command/discovery"
)

// writeGoDirs creates the given directories under root, each with a Go file.
func writeGoDirs(t *testing.T, root string, dirs ...string) {
	t.Helper()
	for _, d := range dirs {
		p := filepath.Join(root, d)
		assert.NilError(t, os.MkdirAll(p, 0o755))
		assert.NilError(t, os.WriteFile(filepath.Join(p, "main.go"), []byte("package "+filepath.Base(d)), 0o600))
	}
}

func TestIterateSetsContainerToFirstGroup(t *testing.T) {
	tmpDir := t.TempDir()
	writeGoDirs(t, tmpDir, "adapter/outbound/redis")

	it, err := discovery.NewPathIterator(tmpDir, "", `adapter/(?P<module>[\w/]+)`)
	assert.NilError(t, err)
	matches, err := it.Iterate()
	assert.NilError(t, err)
	assert.Equal(t, len(matches), 1)
	assert.Equal(t, matches[0].Container, "outbound/redis")
}

func TestHydrateConfigContainerVariable(t *testing.T) {
	cfg := &discovery.PlumberContainerConfig{
		Name:      "{{ .container }}",
		Comment:   "{{ .container_path }}",
		Container: discovery.ContainerPathConfig{Path: "./application_{{ .container_slug }}.go"},
	}
	hydrated := discovery.HydrateConfig(cfg, []discovery.PathMatch{
		{Variables: map[string]string{"module": "outbound/pgsql"}, Container: "outbound/pgsql"},
		{Variables: map[string]string{"module": "outbound/mysql"}, Container: "database"},
	})
	assert.Equal(t, hydrated[0].Name, "OutboundPgsql")
	assert.Equal(t, hydrated[0].Container.Path, "./application_outbound_pgsql.go")
	assert.Equal(t, hydrated[1].Name, "Database")
	assert.Equal(t, hydrated[1].Comment, "database")
	assert.Equal(t, hydrated[1].Container.Path, "./application_database.go")
}

func TestProcessLoopsMergesSharedDependencyContainer(t *testing.T) {
	tmpDir := t.TempDir()
	writeGoDirs(t, tmpDir, "adapter/outbound/pgsql", "adapter/outbound/mysql", "adapter/outbound/redis")

	pgsql := filepath.Join(tmpDir, "adapter/outbound/pgsql")
	mysql := filepath.Join(tmpDir, "adapter/outbound/mysql")
	lookup := func(dir string) (string, bool) {
		if dir == pgsql || dir == mysql {
			return "database", true
		}
		return "", false
	}

	cfg := &discovery.Config{Applications: []discovery.Application{{
		Name: "app",
		Containers: []discovery.Container{{
			PlumberContainer: &discovery.PlumberContainerConfig{
				Name:      "{{ .container }}",
				Container: discovery.ContainerPathConfig{Path: "./application_{{ .container_slug }}.go"},
				Source:    &discovery.SourcePathConfig{Path: "./adapter/{{ .module_path }}/"},
			},
			Loop: &discovery.LoopConfig{Path: `adapter/(?P<module>[\w/]+)`},
		}},
	}}}

	assert.NilError(t, discovery.ProcessLoopsWithContainers(cfg, tmpDir, lookup))

	byName := map[string]*discovery.PlumberContainerConfig{}
	for _, c := range cfg.Applications[0].Containers {
		byName[c.PlumberContainer.Name] = c.PlumberContainer
	}
	assert.Equal(t, len(byName), 2, "pgsql and mysql share the Database container")

	db := byName["Database"]
	assert.Assert(t, db != nil)
	assert.Equal(t, db.Container.Path, "./application_database.go")
	var sources []string
	for _, s := range db.Sources() {
		sources = append(sources, s.Path)
	}
	assert.DeepEqual(t, sources, []string{"./adapter/outbound/mysql/", "./adapter/outbound/pgsql/"})

	redis := byName["OutboundRedis"]
	assert.Assert(t, redis != nil, "without dependency_container the first group is used")
	assert.Equal(t, redis.Container.Path, "./application_outbound_redis.go")
	assert.Equal(t, len(redis.Sources()), 1)
}
