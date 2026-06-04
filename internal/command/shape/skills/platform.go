// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines coding-agent platforms supported by the skills installer
// and provides path-mapping plus filesystem-based autodetection helpers.

package skills

import (
	"fmt"
	"os"
	"path/filepath"
)

// Platform identifies a coding-agent platform that can host installed plumber skills.
type Platform string

const (
	// PlatformAgents installs skills under <dest>/agents/skills/<skill>/.
	PlatformAgents Platform = "agents"
	// PlatformClaude installs skills under <dest>/.claude/skills/<skill>/.
	PlatformClaude Platform = "claude"
	// PlatformCopilot installs skills as flattened single files under
	// <dest>/.github/instructions/<skill>.instructions.md.
	PlatformCopilot Platform = "copilot"
	// PlatformAutodetect expands at runtime to all platforms whose marker
	// directory exists under the destination root.
	PlatformAutodetect Platform = "autodetect"
)

// AllPlatforms returns the concrete (non-autodetect) platforms in stable order.
func AllPlatforms() []Platform {
	return []Platform{PlatformAgents, PlatformClaude, PlatformCopilot}
}

// ParsePlatform validates and returns the Platform for the given string.
func ParsePlatform(s string) (Platform, error) {
	switch Platform(s) {
	case PlatformAgents, PlatformClaude, PlatformCopilot, PlatformAutodetect:
		return Platform(s), nil
	default:
		return "", fmt.Errorf("unknown platform %q (expected: agents, claude, copilot, autodetect)", s)
	}
}

// markerDir returns the directory whose existence under destRoot indicates
// that the platform is in use for the project.
func (p Platform) markerDir() string {
	// nolint: exhaustive //Why: default already handles unknown platforms
	switch p {
	case PlatformAgents:
		return "agents"
	case PlatformClaude:
		return ".claude"
	case PlatformCopilot:
		return ".github"
	default:
		return ""
	}
}

// IsSingleFile reports whether the platform stores each skill as a single
// merged file (true for copilot) rather than as a folder of files.
func (p Platform) IsSingleFile() bool {
	return p == PlatformCopilot
}

// Destination returns the absolute destination path for the given skill
// under the given destination root.
//
// For folder-based platforms (agents, claude) it returns the skill's target
// directory. For single-file platforms (copilot) it returns the merged-file
// path.
func (p Platform) Destination(destRoot, skill string) (string, error) {
	// nolint: exhaustive //Why: default already handles unknown platforms
	switch p {
	case PlatformAgents:
		return filepath.Join(destRoot, "agents", "skills", skill), nil
	case PlatformClaude:
		return filepath.Join(destRoot, ".claude", "skills", skill), nil
	case PlatformCopilot:
		return filepath.Join(destRoot, ".github", "instructions", skill+".instructions.md"), nil
	default:
		return "", fmt.Errorf("platform %q has no destination mapping", p)
	}
}

// DetectPlatforms returns the concrete platforms whose marker directory
// exists under destRoot, in stable order.
func DetectPlatforms(destRoot string) []Platform {
	var found []Platform
	for _, p := range AllPlatforms() {
		marker := p.markerDir()
		if marker == "" {
			continue
		}
		info, err := os.Stat(filepath.Join(destRoot, marker))
		if err == nil && info.IsDir() {
			found = append(found, p)
		}
	}
	return found
}

// ResolvePlatforms expands the requested platform into the concrete set of
// platforms to install into. For PlatformAutodetect, this returns every
// platform whose marker directory exists under destRoot. For any other
// platform it returns a single-element slice.
//
// Returns an error when autodetection finds nothing.
func ResolvePlatforms(requested Platform, destRoot string) ([]Platform, error) {
	if requested != PlatformAutodetect {
		return []Platform{requested}, nil
	}
	detected := DetectPlatforms(destRoot)
	if len(detected) == 0 {
		return nil, fmt.Errorf(
			"autodetect: no platform marker directory found under %q (expected one of: agents/, .claude/, .github/)",
			destRoot,
		)
	}
	return detected, nil
}
