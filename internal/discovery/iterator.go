// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Path iterator for loop config hydration
// Managed: true

package discovery

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
	"unicode"
)

// PathIterator iterates over paths matching a pattern and extracts variables
type PathIterator struct {
	pattern *regexp.Regexp
	baseDir string
}

// PathMatch represents a matched path with extracted variables
type PathMatch struct {
	Path      string
	Variables map[string]string
}

// NewPathIterator creates a new PathIterator from a path pattern
// Pattern format: ./adapter/(?P<module>\w+)/
func NewPathIterator(baseDir, pattern string) (*PathIterator, error) {
	// Convert the pattern to a regex
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid path pattern %q: %w", pattern, err)
	}

	return &PathIterator{
		pattern: re,
		baseDir: baseDir,
	}, nil
}

// Iterate walks the directory tree and yields matches
func (pi *PathIterator) Iterate() ([]PathMatch, error) {
	var matches []PathMatch
	seen := make(map[string]bool) // Track unique matches to avoid duplicates

	// Walk the base directory
	err := filepath.Walk(pi.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Get relative path
		relPath, err := filepath.Rel(pi.baseDir, path)
		if err != nil {
			return err
		}

		// Normalize path separators for matching
		relPath = filepath.ToSlash(relPath)

		// Try to match the pattern
		if submatches := pi.pattern.FindStringSubmatch(relPath); submatches != nil {
			variables := make(map[string]string)

			// Extract named groups
			for i, name := range pi.pattern.SubexpNames() {
				if i > 0 && name != "" && i < len(submatches) {
					variables[name] = submatches[i]
				}
			}

			// Create a unique key from the variables to avoid duplicates
			var key string
			for k, v := range variables {
				key += k + "=" + v + ";"
			}

			// Only add if we haven't seen this combination of variables before
			if !seen[key] {
				seen[key] = true
				matches = append(matches, PathMatch{
					Path:      path,
					Variables: variables,
				})
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to iterate paths: %w", err)
	}

	return matches, nil
}

// HydrateTemplate replaces template variables in a string with actual values using Go templates
// Supports template functions like capitalize, lower, upper
// Example: "{{ module | capitalize }}" -> "Async" when variables contains {"module": "async"}
func HydrateTemplate(templateStr string, variables map[string]string) string {
	// Create template with helper functions
	funcMap := template.FuncMap{
		"capitalize": func(s string) string {
			if s == "" {
				return s
			}
			runes := []rune(s)
			runes[0] = unicode.ToUpper(runes[0])
			return string(runes)
		},
		"upper": strings.ToUpper,
		"lower": strings.ToLower,
		"title": strings.Title,
	}

	tmpl, err := template.New("hydrate").Funcs(funcMap).Parse(templateStr)
	if err != nil {
		// If template parsing fails, return original string
		return templateStr
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, variables); err != nil {
		// If execution fails, return original string
		return templateStr
	}

	return buf.String()
}

// HydrateConfig creates multiple container configs from a template and path matches
func HydrateConfig(cfg *PlumberContainerConfig, matches []PathMatch) []*PlumberContainerConfig {
	var hydrated []*PlumberContainerConfig

	for _, match := range matches {
		// Create a copy of the config
		newCfg := &PlumberContainerConfig{
			Comment: HydrateTemplate(cfg.Comment, match.Variables),
			Name:    HydrateTemplate(cfg.Name, match.Variables),
			Container: ContainerPathConfig{
				Path: HydrateTemplate(cfg.Container.Path, match.Variables),
			},
			Matchers: make([]Matcher, len(cfg.Matchers)),
		}

		// Hydrate source path if present
		if cfg.Source != nil {
			newCfg.Source = &SourcePathConfig{
				Path: HydrateTemplate(cfg.Source.Path, match.Variables),
			}
		}

		// Copy matchers
		copy(newCfg.Matchers, cfg.Matchers)

		hydrated = append(hydrated, newCfg)
	}

	return hydrated
}
