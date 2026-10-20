// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Template loading and checkout orchestrator supporting local, git, and embedded template sources.

package template

import (
	"embed"
	"fmt"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/getoutreach/plumber/internal/genius/gen"
)

// DefaultCacheDir is the default directory used for caching Git repositories
// when checking out templates.
const DefaultCacheDir = "~/.outreach/.plumber"

// Checkout performs Git checkouts for all Git-based template sources, using sparse clones
// to efficiently retrieve only the necessary template files. It returns include results
// pairing each path with its git source config for provenance tracking, plus skill
// directory results for any Skills declarations on git sources.
func Checkout(sources []*SourceConfig, cacheDir string) ([]GitIncludeResult, []GitSkillResult, error) {
	currentDir, err := os.Getwd()
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		err := os.Chdir(currentDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to change back to original directory: %v\n", err)
		}
	}()

	if cacheDir == "" {
		cacheDir = DefaultCacheDir
	}

	if strings.HasPrefix(cacheDir, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, nil, fmt.Errorf("cant' get users home: %w", err)
		}
		cacheDir = path.Join(home, cacheDir[2:])
	}
	var (
		includes []GitIncludeResult
		skills   []GitSkillResult
	)
	for _, s := range sources {
		if s.Git != nil {
			paths, sks, err := checkoutGit(s.Git, cacheDir)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to checkout git source %s: %w", s.Git.Repository, err)
			}
			for _, p := range paths {
				includes = append(includes, GitIncludeResult{Path: p, Git: s.Git})
			}
			skills = append(skills, sks...)
		}
	}
	return includes, skills, nil
}

// LoadAllContent converts all inline content templates into render option functions.
// This is used by commands like discovery where content templates provide override
// blocks ({{define "..."}}) rather than being referenced by name.
func LoadAllContent(content []ContentConfig) ([]gen.RenderOptionsFunc, error) {
	var opts []gen.RenderOptionsFunc
	for _, c := range content {
		t, err := template.New(c.Name).Parse(c.Content)
		if err != nil {
			return nil, fmt.Errorf("failed to parse content template %q: %w", c.Name, err)
		}
		opts = append(opts, gen.WithTemplate(t))
	}
	return opts, nil
}

// ResolveRefs resolves a list of ContentConfig references into render options.
// Each ref is either:
//   - Inline content (Content is non-empty): parsed as a template directly.
//   - Name reference (Content is empty): resolved from the cache.
func ResolveRefs(
	cache *TemplateCache,
	refs []ContentConfig,
) ([]gen.RenderOptionsFunc, error) {
	var opts []gen.RenderOptionsFunc
	for i, ref := range refs {
		if ref.Content != "" {
			// Inline content — use WithNamedTemplateContent so that {{define}} blocks
			// are parsed into the root template with funcmaps already available.
			opts = append(opts, gen.WithNamedTemplateContent(inlineSourceName(ref.Name, i), ref.Content))
		} else {
			// Name reference — resolve from cache
			resolved, err := cache.Load(ref.Name)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve template %q: %w", ref.Name, err)
			}
			opts = append(opts, resolved...)
		}
	}
	return opts, nil
}

// TemplateCache provides a memoizing wrapper around template resolution. It
// loads each template name at most once and caches the resulting render options
// so that repeated lookups (e.g. across many transformations) avoid redundant
// file I/O and template parsing.
//
//nolint:revive //Why: We might rename package
type TemplateCache struct {
	sources  []*SourceConfig
	content  []ContentConfig
	cacheDir string
	fs       embed.FS
	cache    map[string][]gen.RenderOptionsFunc
}

// NewTemplateCache creates a TemplateCache backed by the given sources,
// inline content definitions, cache directory (for git repos) and embedded FS.
func NewTemplateCache(sources []*SourceConfig, content []ContentConfig, cacheDir string, fs embed.FS) *TemplateCache {
	return &TemplateCache{
		sources:  sources,
		content:  content,
		cacheDir: cacheDir,
		fs:       fs,
		cache:    make(map[string][]gen.RenderOptionsFunc),
	}
}

// Load resolves the given template names into render option functions,
// returning cached results when available and resolving (then caching)
// any names seen for the first time.
func (tc *TemplateCache) Load(name string, names ...string) ([]gen.RenderOptionsFunc, error) {
	names = append([]string{name}, names...)
	var opts []gen.RenderOptionsFunc
	for _, name := range names {
		if name == "" {
			continue
		}
		if cached, ok := tc.cache[name]; ok {
			opts = append(opts, cached...)
			continue
		}
		resolved, err := resolveTemplate(tc.sources, tc.content, tc.cacheDir, name, tc.fs)
		if err != nil {
			return nil, err
		}
		tc.cache[name] = resolved
		opts = append(opts, resolved...)
	}
	return opts, nil
}

// resolveTemplate resolves a single template name into render option functions
// by searching embedded templates, sources and inline content in order.
func resolveTemplate(
	sources []*SourceConfig,
	content []ContentConfig,
	cacheDir string,
	name string,
	fs embed.FS,
) ([]gen.RenderOptionsFunc, error) {
	if strings.HasPrefix(name, "plumber:") {
		trimmed := strings.TrimPrefix(name, "plumber:")
		if !strings.HasPrefix(trimmed, "templates/") {
			trimmed = "templates/" + trimmed
		}
		if !strings.HasSuffix(trimmed, ".gtpl") {
			trimmed += ".gtpl"
		}
		return []gen.RenderOptionsFunc{gen.WithFS(fs, trimmed)}, nil
	}

	var opts []gen.RenderOptionsFunc
	found := false
	for _, s := range sources {
		switch {
		case s.Git != nil:
			repoPath := gitRepoPath(cacheDir, s.Git)
			for _, tpl := range s.Git.Templates {
				if tpl.Name != name {
					continue
				}
				found = true
				filename := path.Join(repoPath, tpl.Path)
				data, err := os.ReadFile(filename)
				if err != nil {
					return nil, fmt.Errorf("failed to read git template file %q: %w", filename, err)
				}
				opts = append(opts, gen.WithNamedTemplateContent(gitSourceName(s.Git, tpl.Path, filename), string(data)))
			}
		case s.Local != nil:
			for _, tpl := range s.Local.Templates {
				if tpl.Name != name {
					continue
				}
				found = true
				tplPath := tpl.Path
				if tplPath == "" {
					tplPath = name + ".gtpl"
				}
				t, err := template.New(tpl.Name).ParseFiles(path.Join(s.Local.Path, tplPath))
				if err != nil {
					return nil, fmt.Errorf("Can't load local template: %w", err)
				}
				opts = append(opts, gen.WithTemplate(t))
			}
		default:
			return nil, fmt.Errorf("Unsupported template configuration for %T", s)
		}
	}
	for i, s := range content {
		if s.Name == name {
			found = true
			opts = append(opts, gen.WithNamedTemplateContent(inlineSourceName(s.Name, i), s.Content))
		}
	}
	if !found {
		return nil, fmt.Errorf("template %q not found", name)
	}
	return opts, nil
}

// gitSourceName builds the template parse name for a template file loaded from
// a git source. It is reported in template error locations, so it contains the
// repository, ref and path within the repository as well as the local file
// that can be opened for inspection.
func gitSourceName(g *GitSourceConfig, tplPath, filename string) string {
	repo := g.Repository
	if g.Ref != "" {
		repo += "@" + g.Ref
	}
	return fmt.Sprintf("%s/%s (%s)", repo, strings.TrimPrefix(tplPath, "/"), filename)
}

// inlineSourceName builds the template parse name for inline template content
// from the config. It is reported in template error locations.
func inlineSourceName(name string, index int) string {
	if name == "" {
		return fmt.Sprintf("content[%d]", index)
	}
	return fmt.Sprintf("content[%s]", name)
}

// LoadTemplates resolves template names to render option functions by searching through
// sources in priority order: embedded (plumber: prefix), git/local sources, inline content.
func LoadTemplates(
	sources []*SourceConfig,
	content []ContentConfig,
	cacheDir string,
	names []string,
	fs embed.FS,
) ([]gen.RenderOptionsFunc, error) {
	var opts []gen.RenderOptionsFunc
	for _, name := range names {
		resolved, err := resolveTemplate(sources, content, cacheDir, name, fs)
		if err != nil {
			return nil, err
		}
		opts = append(opts, resolved...)
	}
	return opts, nil
}
