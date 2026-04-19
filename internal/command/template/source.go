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
// to efficiently retrieve only the necessary template files. It returns include paths
// found within git repos (for merging additional config).
func Checkout(sources []SourceConfig, cacheDir string) ([]string, error) {
	currentDir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	defer func() {
		err := os.Chdir(currentDir)
		if err != nil {
			fmt.Printf("warning: failed to change back to original directory: %v\n", err)
		}
	}()

	if cacheDir == "" {
		cacheDir = DefaultCacheDir
	}

	if strings.HasPrefix(cacheDir, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("cant' get users home: %w", err)
		}
		cacheDir = path.Join(home, cacheDir[2:])
	}
	fmt.Println("Using cacheDir:", cacheDir)
	var includePaths []string
	for _, s := range sources {
		if s.Git != nil {
			paths, err := checkoutGit(s.Git, cacheDir)
			if err != nil {
				return nil, fmt.Errorf("failed to checkout git source %s: %w", s.Git.Repository, err)
			}
			includePaths = append(includePaths, paths...)
		}
	}
	return includePaths, nil
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
//   - Name reference (Content is empty): resolved from the registry (sources + content)
//     via LoadTemplates.
func ResolveRefs(
	refs []ContentConfig,
	sources []SourceConfig,
	registry []ContentConfig,
	cacheDir string,
	fs embed.FS,
) ([]gen.RenderOptionsFunc, error) {
	var opts []gen.RenderOptionsFunc
	for _, ref := range refs {
		if ref.Content != "" {
			// Inline content — use WithTemplateContent so that {{define}} blocks
			// are parsed into the root template with funcmaps already available.
			opts = append(opts, gen.WithTemplateContent(ref.Content))
		} else {
			// Name reference — resolve from registry (sources + content)
			resolved, err := LoadTemplates(sources, registry, cacheDir, []string{ref.Name}, fs)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve template %q: %w", ref.Name, err)
			}
			opts = append(opts, resolved...)
		}
	}
	return opts, nil
}

// LoadTemplates resolves template names to render option functions by searching through
// sources in priority order: embedded (plumber: prefix), git/local sources, inline content.
func LoadTemplates(
	sources []SourceConfig,
	content []ContentConfig,
	cacheDir string,
	names []string,
	fs embed.FS,
) ([]gen.RenderOptionsFunc, error) {
	opts := []gen.RenderOptionsFunc{}
	for _, name := range names {
		if strings.HasPrefix(name, "plumber:") {
			name = strings.TrimPrefix(name, "plumber:")
			if !strings.HasPrefix(name, "templates/") {
				name = "templates/" + name
			}
			if !strings.HasSuffix(name, ".gtpl") {
				name += ".gtpl"
			}
			opts = append(opts, gen.WithFS(fs, name))
			continue
		}
		for _, s := range sources {
			switch {
			case s.Git != nil:
				repoPath := gitRepoPath(cacheDir, s.Git.Repository, s.Git.Ref)
				for _, tpl := range s.Git.Templates {
					if tpl.Name == name {
						t, err := template.New(tpl.Name).ParseFiles(path.Join(repoPath, tpl.Path))
						if err != nil {
							return nil, fmt.Errorf("Can't load git template: %w", err)
						}
						opts = append(opts, gen.WithTemplate(t))
					}
				}
			case s.Local != nil:
				for _, tpl := range s.Local.Templates {
					if tpl.Name != name {
						continue
					}
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
		for _, s := range content {
			if s.Name == name {
				opts = append(opts, gen.WithTemplateContent(s.Content))
			}
		}
	}

	return opts, nil
}
