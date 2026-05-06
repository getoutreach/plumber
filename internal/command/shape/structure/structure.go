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

// StructurePathResolver defines the interface for resolving structure paths according to the provided configuration.
const (
	// StructurePathPrefix is the prefix used to identify structure paths in the shape command configuration.
	StructurePathPrefix = "structure:"
)

// Resolver resolves structure paths according to the provided configuration.
type Resolver struct {
	config     *config.PlumberStructureConfig
	repoModule contract.ModuleInfo
	module     contract.ModuleInfo
}

// NewResolver creates a new Resolver with the given structure name.
func NewResolver(
	structureConfig *config.PlumberStructureConfig, repoModule, module contract.ModuleInfo) (contract.StructurePathResolver, error) {
	if structureConfig == nil {
		return &NoopResolver{}, nil
	}

	err := expand.Structure(structureConfig, repoModule, module)
	if err != nil {
		return nil, fmt.Errorf("failed to expand structure paths: %w", err)
	}

	return &Resolver{
		config:     structureConfig,
		repoModule: repoModule,
		module:     module,
	}, nil
}

// NoopResolver is a no-op implementation of the Resolver that simply returns the path as-is.
type NoopResolver struct{}

func (r *NoopResolver) ResolvePath(p string) (string, error) {
	return p, nil
}

func (r *Resolver) ResolvePath(p string) (string, error) {
	if strings.HasPrefix(p, StructurePathPrefix) {
		p = strings.TrimPrefix(p, StructurePathPrefix)
		sp, found := lo.Find(r.config.Paths, func(pt config.StructurePathConfig) bool {
			return pt.Path.Name == p
		})
		if found {
			return path.Join(r.module.Path, r.config.Path, sp.Path.Path), nil
		}

		names := lo.Map(r.config.Paths, func(pt config.StructurePathConfig, _ int) string {
			return pt.Path.Name
		})

		// If not found, return an error or the original path. Here we choose to return the original path.
		return p, fmt.Errorf("structure path '%s' not found in configuration. Available paths: %v", p, names)
	}

	// For demonstration purposes, this resolver simply returns the path as-is.
	// In a real implementation, this would contain logic to resolve the path according to the structure configuration.
	return p, nil
}
