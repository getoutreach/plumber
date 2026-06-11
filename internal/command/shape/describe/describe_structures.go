// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file builds a structured description of every registered
// shape structure and its named paths from a fully-merged shape configuration.
// Template variables in structure paths are expanded via expand.Structure so
// the output reflects the resolved filesystem layout for the current module.

package describe

import (
	pathpkg "path"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/expand"
	"github.com/getoutreach/plumber/internal/command/shape/structure"
)

// StructuresDescription is the top-level output structure for the structures
// describe subcommand. It enumerates every registered structure and its
// named paths.
type StructuresDescription struct {
	Structures []StructureFullDescription `json:"structures" yaml:"structures"`
}

// StructureFullDescription describes a single registered structure together
// with all of its named paths.
type StructureFullDescription struct {
	Name          string                     `json:"name" yaml:"name"`
	Title         string                     `json:"title,omitempty" yaml:"title,omitempty"`
	Documentation string                     `json:"documentation,omitempty" yaml:"documentation,omitempty"`
	BasePath      string                     `json:"basePath,omitempty" yaml:"basePath,omitempty"`
	Paths         []StructurePathDescription `json:"paths,omitempty" yaml:"paths,omitempty"`
}

// StructurePathDescription describes a single named path within a structure.
// Name and Documentation come directly from the underlying
// PlumberStructurePathConfig and are surfaced as first-class fields so callers
// (skill templates, JSON consumers) can reference them without inspecting the
// raw path object. RelativePath is the location of this path relative to the
// module root after template expansion. Usage holds the canonical reference
// syntax (structure:<name>) used inside annotation values.
type StructurePathDescription struct {
	Name               string `json:"name" yaml:"name"`
	Title              string `json:"title,omitempty" yaml:"title,omitempty"`
	PackageDescription string `json:"packageDescription,omitempty" yaml:"packageDescription,omitempty"`
	Documentation      string `json:"documentation,omitempty" yaml:"documentation,omitempty"`
	Usage              string `json:"usage,omitempty" yaml:"usage,omitempty"`
	RelativePath       string `json:"relativePath,omitempty" yaml:"relativePath,omitempty"`
	Required           bool   `json:"required,omitempty" yaml:"required,omitempty"`
}

// BuildStructures extracts every available structure (and its named paths)
// from cfg into a StructuresDescription. Structures are deep-copied and run
// through expand.Structure with repoModule and module so that template
// variables in structure paths (for example {{ .Module.NormalizedName }})
// are rendered into concrete filesystem paths.
//
// Resolution preference: when cfg.StructureDefinitions has been populated
// (the normal CLI path) those are used; otherwise the function falls back
// to the raw cfg.Structures so unconfigured projects still emit something
// useful. The resolver argument is accepted for symmetry with describe.Build
// and is currently unused — expansion happens directly via expand.Structure
// to avoid relying on resolver state that may be a NoopResolver.
func BuildStructures(
	cfg *shape.Config,
	_ contract.StructurePathResolver,
	repoModule, module contract.ModuleInfo,
) (StructuresDescription, error) {
	structs := selectStructures(cfg)
	out := StructuresDescription{Structures: make([]StructureFullDescription, 0, len(structs))}
	for _, s := range structs {
		expanded := copyStructure(s)
		// expand.Structure is idempotent (no-op when no templates remain) so
		// it is safe to call even when cfg.StructureDefinitions has already
		// been processed by the resolver.
		err := expand.Structure(&expanded, repoModule, module)
		if err != nil {
			return StructuresDescription{}, err
		}
		out.Structures = append(out.Structures, buildStructureFull(expanded))
	}
	return out, nil
}

// selectStructures prefers the resolved structure definitions when present
// and falls back to the raw structures list from configuration.
func selectStructures(cfg *shape.Config) []config.PlumberStructureConfig {
	if cfg.StructureDefinitions != nil && len(cfg.StructureDefinitions.Structures) > 0 {
		return cfg.StructureDefinitions.Structures
	}
	out := make([]config.PlumberStructureConfig, 0, len(cfg.Structures))
	for _, def := range cfg.Structures {
		if def == nil {
			continue
		}
		out = append(out, def.Structure)
	}
	return out
}

// copyStructure returns a deep-enough copy of s so that subsequent template
// expansion does not mutate the caller's configuration.
func copyStructure(s config.PlumberStructureConfig) config.PlumberStructureConfig {
	paths := make([]config.StructurePathConfig, len(s.Paths))
	copy(paths, s.Paths)
	s.Paths = paths
	return s
}

// buildStructureFull converts a (post-expansion) PlumberStructureConfig into
// its description form.
func buildStructureFull(s config.PlumberStructureConfig) StructureFullDescription {
	desc := StructureFullDescription{
		Name:          s.Name,
		Title:         s.Title,
		Documentation: s.Documentation,
		BasePath:      s.Path,
		Paths:         make([]StructurePathDescription, 0, len(s.Paths)),
	}
	for _, p := range s.Paths {
		desc.Paths = append(desc.Paths, buildStructurePath(s, p))
	}
	return desc
}

// buildStructurePath converts a StructurePathConfig (within parent) into its
// description form, computing the module-relative path and the canonical
// usage reference. Name and Documentation are propagated verbatim from the
// underlying PlumberStructurePathConfig.
func buildStructurePath(parent config.PlumberStructureConfig, p config.StructurePathConfig) StructurePathDescription {
	relative := pathpkg.Join(parent.Path, p.Path.Path)
	if relative == "." {
		relative = ""
	}
	return StructurePathDescription{
		Name:               p.Path.Name,
		Title:              p.Path.Title,
		PackageDescription: p.Path.PackageDescription,
		Documentation:      p.Path.Documentation,
		Usage:              structure.StructurePathPrefix + p.Path.Name,
		RelativePath:       relative,
		Required:           p.Path.Required,
	}
}
