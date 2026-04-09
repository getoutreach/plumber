// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements template loading and checkout for the shape command, supporting local, git, and embedded template sources.

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

const DefaultCacheDir = "~/.outreach/.plumber"

func Checkout(cfg *contract.PlumberTemplatesConfig, cacheDir string) error {
	currentDir, err := os.Getwd()
	if err != nil {
		return err
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
			return fmt.Errorf("cant' get users home: %w", err)
		}
		cacheDir = path.Join(home, cacheDir[2:])
	}
	fmt.Println("Using cacheDir:", cacheDir)
	for _, s := range cfg.Sources {
		if s.Git != nil {
			checkoutGit(s.Git, cacheDir)
		}
	}
	return nil
}

func Load(cfg *contract.PlumberTemplatesConfig, cacheDir string, names []string, fs embed.FS) ([]gen.RenderOptionsFunc, error) {
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
		for _, s := range cfg.Sources {
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
					if tpl.Name == name {
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
