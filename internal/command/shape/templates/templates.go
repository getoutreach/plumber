// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements template loading and checkout for the shape command,
// supporting local, git, and embedded template sources.

// Package templates provides template loading and Git checkout utilities for shape command template sources.
package templates

import (
	"embed"
	"fmt"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/genius/gen"
)

// DefaultCacheDir is the default directory used for caching Git repositories when checking out templates for the shape command.
const DefaultCacheDir = "~/.outreach/.plumber"

func Checkout(sources []contract.PlumberTemplateSourceConfig, cacheDir string) ([]string, error) {
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

func Load(
	sources []contract.PlumberTemplateSourceConfig,
	cfg *contract.PlumberTemplatesConfig,
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
		for _, s := range cfg.Content {
			if s.Name == name {
				t, err := template.New(s.Name).Parse(s.Content)
				if err != nil {
					return nil, err
				}
				opts = append(opts, gen.WithTemplate(t))
			}
		}
	}

	return opts, nil
}
