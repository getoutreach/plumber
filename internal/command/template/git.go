// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Git-based template source checkout using sparse clones.

package template

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"github.com/samber/lo"
)

func checkoutGit(cfg *GitSourceConfig, cacheDir string) ([]string, error) {
	if cfg.Ref == "" {
		cfg.Ref = "main"
	}
	repoPath := cfg.Replaced
	if cfg.Replaced == "" {
		repoPath = gitRepoPath(cacheDir, cfg)

		err := os.MkdirAll(repoPath, os.ModePerm)
		if err != nil {
			return nil, err
		}

		isGitRepo, err := exists(path.Join(repoPath, ".git"))
		if err != nil {
			return nil, err
		}

		// init repo if it doesn't exist
		if !isGitRepo {
			cmd := execCommand("git", "clone", "--no-checkout", "--depth=1", "--filter=tree:0", "--branch="+cfg.Ref, cfg.Repository, repoPath)
			stdout, err := cmd.Output()
			if err != nil {
				return nil, err
			}
			fmt.Println(string(stdout))
		}

		err = os.Chdir(repoPath)
		if err != nil {
			return nil, err
		}

		dirs := lo.Map(cfg.Templates, func(t FileRef, _ int) string {
			p := path.Dir(t.Path)
			p = strings.TrimPrefix(p, "./")
			p = strings.TrimPrefix(p, "/")
			return "/" + p
		})

		// Also include directories for include paths so they get checked out.
		for _, inc := range cfg.Includes {
			p := path.Dir(inc.Path)
			p = strings.TrimPrefix(p, "./")
			p = strings.TrimPrefix(p, "/")
			dirs = append(dirs, "/"+p)
		}

		cmd := execCommand("git", append([]string{"sparse-checkout", "set", "--no-cone"}, dirs...)...)
		stdout, err := cmd.Output()
		if err != nil {
			return nil, err
		}
		fmt.Fprintln(os.Stderr, string(stdout))

		cmd = execCommand("git", "checkout", "origin/"+cfg.Ref)
		stdout, err = cmd.Output()
		if err != nil {
			return nil, err
		}
		fmt.Fprintln(os.Stderr, string(stdout))
	}

	var expandError []error
	// Expand templates with globs into individual file paths.
	cfg.Templates = lo.FlatMap(cfg.Templates, func(f FileRef, _ int) []FileRef {
		fmt.Println(f.Path)
		if !strings.Contains(f.Path, "*") {
			return []FileRef{f}
		}
		matches, err := filepath.Glob(filepath.Join(repoPath, f.Path))
		if err != nil {
			expandError = append(expandError, fmt.Errorf("failed to glob template path %q in repo %s: %w", f.Path, cfg.Repository, err))
			return nil
		}
		return lo.Map(matches, func(m string, _ int) FileRef {
			rel, err := filepath.Rel(repoPath, m)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				expandError = append(expandError, fmt.Errorf("failed to get relative path for glob match %q in repo %s: %w", m, cfg.Repository, err))
				return FileRef{}
			}

			parts := strings.Split(f.Path, "*")

			name := strings.TrimPrefix(rel, parts[0])
			name = strings.TrimSuffix(name, path.Ext(name))

			return FileRef{Name: name, Path: m}
		})
	})
	if expandError != nil {
		return nil, errors.Join(expandError...)
	}

	// Resolve include globs within the checked-out repo.
	var includePaths []string
	for _, inc := range cfg.Includes {
		matches, err := filepath.Glob(filepath.Join(repoPath, inc.Path))
		if err != nil {
			return nil, fmt.Errorf("failed to glob include path %q in repo %s: %w", inc.Path, cfg.Repository, err)
		}
		includePaths = append(includePaths, matches...)
	}

	return includePaths, nil
}

func gitRepoPath(cacheDir string, cfg *GitSourceConfig) string {
	if cfg.Replaced != "" {
		return cfg.Replaced
	}
	if cfg.Ref == "" {
		cfg.Ref = "main"
	}
	repository := strings.TrimPrefix(cfg.Repository, "https://")
	repository = strings.TrimPrefix(repository, "git@")
	repository = strings.ReplaceAll(repository, "/", "-")
	return path.Join(cacheDir, repository, cfg.Ref)
}

func exists(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func execCommand(name string, args ...string) *exec.Cmd {
	fmt.Fprintln(os.Stderr, name, strings.Join(args, " "))
	return exec.Command(name, args...)
}
