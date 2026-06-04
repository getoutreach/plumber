// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Path iterator for loop config hydration
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

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// PathIterator iterates over paths matching a pattern and extracts variables
type PathIterator struct {
	pattern     *regexp.Regexp
	baseDir     string
	loopBaseDir string
}

// PathMatch represents a matched path with extracted variables
type PathMatch struct {
	Path      string
	Variables map[string]string
}

// NewPathIterator creates a new PathIterator from a path pattern
// Pattern format: ./adapter/(?P<module>[\w/]+)
func NewPathIterator(baseDir, loopBaseDir, pattern string) (*PathIterator, error) {
	// Convert the pattern to a regex
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid path pattern %q: %w", pattern, err)
	}

	return &PathIterator{
		pattern:     re,
		baseDir:     baseDir,
		loopBaseDir: loopBaseDir,
	}, nil
}

// Iterate walks the directory tree and yields matches.
// Only directories containing .go files are included — intermediate directories
// (e.g., adapter/outbound/ when only adapter/outbound/redis/ has Go files) are skipped.
func (pi *PathIterator) Iterate() ([]PathMatch, error) {
	var (
		matches        []PathMatch
		seen           = make(map[string]bool) // Track unique matches to avoid duplicates
		err            error
		absLoopBaseDir string
	)

	if pi.loopBaseDir != "" {
		absLoopBaseDir, err = filepath.Abs(pi.loopBaseDir)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path for loopBaseDir %q: %w", pi.loopBaseDir, err)
		}

		_, err = filepath.Rel(pi.baseDir, absLoopBaseDir)
		if err != nil {
			return nil, fmt.Errorf("failed to get relative path: %w for baseDir %q and loopBaseDir %q", err, pi.baseDir, pi.loopBaseDir)
		}
	} else {
		absLoopBaseDir = pi.baseDir
	}

	// Walk the base directory
	err = filepath.Walk(absLoopBaseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Only consider directories
		if !info.IsDir() {
			return nil
		}

		// Get relative path
		relPath, err := filepath.Rel(pi.baseDir, path)
		if err != nil {
			return err
		}

		// Normalize path separators for matching
		relPath = filepath.ToSlash(relPath)

		// Try to match the pattern
		submatches := pi.pattern.FindStringSubmatch(relPath)
		if submatches == nil {
			return nil
		}

		// Skip directories that contain no .go files
		if !dirHasGoFiles(path) {
			return nil
		}

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

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk paths: %w", err)
	}

	return matches, nil
}

// dirHasGoFiles reports whether the directory at path contains at least one .go file
// (non-recursive — only direct children).
func dirHasGoFiles(path string) bool {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".go" {
			return true
		}
	}
	return false
}

// TemplateFuncMap returns the shared template function map used by both
// HydrateTemplate and any other template rendering in the discovery system.
var TemplateFuncMap = template.FuncMap{
	// capitalize splits on "/" and capitalizes the first letter of each segment,
	// then joins them. "outbound/redis" -> "OutboundRedis", "async" -> "Async".
	"capitalize": Capitalize,
	"upper":      strings.ToUpper,
	"lower":      strings.ToLower,
	"title":      cases.Title(language.Und).String,
	// slug replaces "/" with "_". "outbound/redis" -> "outbound_redis".
	"slug": Slug,
}

func capitalize(s, sep string) string {
	if s == "" {
		return s
	}
	parts := strings.Split(s, "/")
	for i, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(part)
		runes[0] = unicode.ToUpper(runes[0])
		parts[i] = string(runes)
	}
	return strings.Join(parts, sep)
}

// Capitalize converts a (possibly path-separated) string to PascalCase.
// Each segment separated by "/" has its first letter uppercased, then segments are joined.
//
//	"async"          -> "Async"
//	"outbound/redis" -> "OutboundRedis"
func Capitalize(s string) string {
	return capitalize(s, "")
}

// Capitalize converts a (possibly path-separated) string to PascalCase.
// Each segment separated by "/" has its first letter uppercased, then segments are joined.
//
//	"async"          -> "Async"
//	"outbound/redis" -> "Outbound_Redis"
func CapitalizeSnake(s string) string {
	return capitalize(s, "_")
}

// Slug replaces "/" with "_" in a string.
//
//	"Outbound/Redis" -> "outbound_redis"
//	"async"          -> "async"
func Slug(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "/", "_"))
}

// HydrateTemplate replaces template variables in a string with actual values using Go templates.
// Supports template functions: capitalize, upper, lower, title, slug.
func HydrateTemplate(templateStr string, variables map[string]string) string {
	tmpl, err := template.New("hydrate").Funcs(TemplateFuncMap).Parse(templateStr)
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

// DeriveVariables expands raw captured variables into their derived forms.
// For each variable "X" with value "v":
//   - "X"       -> PascalCase (Capitalize): "outbound/redis" -> "OutboundRedis"
//   - "X_slug"  -> underscore-separated:    "outbound/redis" -> "outbound_redis"
//   - "X_path"  -> raw captured value:      "outbound/redis" -> "outbound/redis"
func DeriveVariables(raw map[string]string) map[string]string {
	derived := make(map[string]string, len(raw)*3)
	for k, v := range raw {
		derived[k] = Capitalize(v)
		derived[k+"_slug"] = Slug(v)
		derived[k+"_path"] = v
	}
	return derived
}

// HydrateConfig creates multiple container configs from a template and path matches.
// Variables are automatically expanded with derived forms (_slug, _path) before hydration.
func HydrateConfig(cfg *PlumberContainerConfig, matches []PathMatch) []*PlumberContainerConfig {
	var hydrated []*PlumberContainerConfig

	for _, match := range matches {
		vars := DeriveVariables(match.Variables)

		// Create a copy of the config
		newCfg := &PlumberContainerConfig{
			Comment: HydrateTemplate(cfg.Comment, vars),
			Name:    HydrateTemplate(cfg.Name, vars),
			Container: ContainerPathConfig{
				Path: HydrateTemplate(cfg.Container.Path, vars),
			},
			Matchers: make([]Matcher, len(cfg.Matchers)),
		}

		// Hydrate source path if present
		if cfg.Source != nil {
			newCfg.Source = &SourcePathConfig{
				Path: HydrateTemplate(cfg.Source.Path, vars),
			}
		}

		// Copy matchers
		copy(newCfg.Matchers, cfg.Matchers)

		hydrated = append(hydrated, newCfg)
	}

	return hydrated
}
