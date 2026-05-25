// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the structure resolvers

// Package structure provides utilities for resolving structure paths according to structure path configuration.
package structure

import (
	"fmt"
	"path"
	"strings"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/expand"
	"github.com/samber/lo"
)

// StructurePathPrefix is the prefix used to identify structure paths in the shape command configuration.
const StructurePathPrefix = "structure:"

// Resolver resolves structure paths by searching across multiple structure definitions.
// The first matching path name across all structures wins.
type Resolver struct {
	definitions *config.StructureDefinitions
	repoModule  contract.ModuleInfo
	module      contract.ModuleInfo
}

// NewResolver creates a new Resolver for the given structure definitions.
// If definitions is nil or empty, a NoopResolver is returned.
func NewResolver(
	definitions *config.StructureDefinitions, repoModule, module contract.ModuleInfo,
) (contract.StructurePathResolver, error) {
	if definitions == nil || len(definitions.Structures) == 0 {
		return &NoopResolver{}, nil
	}

	for i := range definitions.Structures {
		err := expand.Structure(&definitions.Structures[i], repoModule, module)
		if err != nil {
			return nil, fmt.Errorf("failed to expand structure %q paths: %w", definitions.Structures[i].Name, err)
		}
	}

	return &Resolver{
		definitions: definitions,
		repoModule:  repoModule,
		module:      module,
	}, nil
}

// NoopResolver is a no-op implementation of the Resolver that simply returns the path as-is.
type NoopResolver struct{}

func (r *NoopResolver) ResolvePackagePath(p string) (string, error) {
	return p, nil
}

func (r *NoopResolver) ResolveStructurePath(p string) (string, error) {
	return p, nil
}

func (r *Resolver) resolve(p string) (
	mod *contract.ModuleInfo,
	structureConfig *config.PlumberStructureConfig,
	structurePath *config.StructurePathConfig,
	err error,
) {
	if !strings.HasPrefix(p, StructurePathPrefix) {
		return nil, nil, nil, nil
	}
	name := strings.TrimPrefix(p, StructurePathPrefix)

	// Search across all structures, first match wins.
	for _, s := range r.definitions.Structures {
		sp, found := lo.Find(s.Paths, func(pt config.StructurePathConfig) bool {
			return pt.Path.Name == name
		})
		if found {
			return &r.module, &s, &sp, nil
		}
	}

	// Collect all available path names for the error message.
	var allNames []string
	for _, s := range r.definitions.Structures {
		names := lo.Map(s.Paths, func(pt config.StructurePathConfig, _ int) string {
			return s.Name + "/" + pt.Path.Name
		})
		allNames = append(allNames, names...)
	}

	return nil, nil, nil, fmt.Errorf("structure path '%s' not found in any structure definition. Available paths: %v", name, allNames)
}

func (r *Resolver) ResolveStructurePath(p string) (string, error) {
	m, sc, sp, err := r.resolve(p)
	if err != nil {
		return "", err
	}
	if m == nil {
		return p, nil
	}

	return path.Join(m.Dir, sc.Path, sp.Path.Path), nil
}

func (r *Resolver) ResolvePackagePath(p string) (string, error) {
	m, sc, sp, err := r.resolve(p)
	if err != nil {
		return "", err
	}
	if m == nil {
		return p, nil
	}

	return path.Join(m.Path, sc.Path, sp.Path.Path), nil
}
