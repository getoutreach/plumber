// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the lookup of dependency container names defined on structure paths.

package structure

import (
	"fmt"
	"path/filepath"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/expand"
)

// DependencyContainerLookup resolves the dependency container name configured
// (plumber.path dependency_container) for a directory on disk. It returns false
// when the directory is not a structure path or the path has no dependency
// container defined.
type DependencyContainerLookup func(dir string) (string, bool)

// NewDependencyContainerLookup expands the given structure definitions against
// the repo/module info and builds a lookup from the absolute directory of every
// structure path (module dir + structure path + plumber.path path) to its
// dependency container name. Multiple structure paths can share the same
// dependency container. The definitions are not modified.
func NewDependencyContainerLookup(
	definitions *config.StructureDefinitions, repoModule, module contract.ModuleInfo,
) (DependencyContainerLookup, error) {
	containers := map[string]string{}
	if definitions != nil {
		for _, s := range definitions.Structures {
			// Work on a copy so expansion does not mutate the caller's definitions.
			sc := s
			sc.Paths = append([]config.StructurePathConfig(nil), s.Paths...)
			if err := expand.Structure(&sc, repoModule, module); err != nil {
				return nil, fmt.Errorf("failed to expand structure %q paths: %w", sc.Name, err)
			}
			for _, p := range sc.Paths {
				if p.Path.DependencyContainer == "" {
					continue
				}
				dir := filepath.Clean(filepath.Join(module.Dir, sc.Path, p.Path.Path))
				// First definition wins, consistent with structure path resolution.
				if _, exists := containers[dir]; !exists {
					containers[dir] = p.Path.DependencyContainer
				}
			}
		}
	}
	return func(dir string) (string, bool) {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return "", false
		}
		c, ok := containers[filepath.Clean(abs)]
		return c, ok
	}, nil
}
