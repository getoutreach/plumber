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

// checkoutGit materializes a remote git source into the cache directory and
// resolves the templates, includes, and skills declared on cfg against the
// on-disk checkout. cfg.Templates is mutated in place to expand glob patterns
// to concrete files; resolved include paths and skill directories are
// returned.
func checkoutGit(cfg *GitSourceConfig, cacheDir string) ([]string, []GitSkillResult, error) {
	if cfg.Ref == "" {
		cfg.Ref = "main"
	}

	repoPath, err := prepareRepoCheckout(cfg, cacheDir)
	if err != nil {
		return nil, nil, err
	}

	if err := expandTemplateGlobs(cfg, repoPath); err != nil {
		return nil, nil, err
	}

	includes, err := resolveIncludePaths(cfg, repoPath)
	if err != nil {
		return nil, nil, err
	}

	skillResults, err := resolveSkills(cfg, repoPath)
	if err != nil {
		return nil, nil, err
	}

	return includes, skillResults, nil
}

// prepareRepoCheckout returns the local path of the repository, cloning the
// remote and applying sparse checkout when no replacement directory is in
// effect. When cfg.Replaced is set the path is returned unchanged with no
// I/O — the caller has already provided a ready-to-use checkout.
func prepareRepoCheckout(cfg *GitSourceConfig, cacheDir string) (string, error) {
	if cfg.Replaced != "" {
		return cfg.Replaced, nil
	}
	repoPath := gitRepoPath(cacheDir, cfg)

	if err := ensureClone(cfg, repoPath); err != nil {
		return "", err
	}
	if err := os.Chdir(repoPath); err != nil {
		return "", err
	}
	if err := applySparseCheckout(cfg); err != nil {
		return "", err
	}
	return repoPath, nil
}

// ensureClone makes the repo directory and performs a shallow no-checkout
// clone of cfg.Repository when the directory is not yet a git checkout.
func ensureClone(cfg *GitSourceConfig, repoPath string) error {
	if err := os.MkdirAll(repoPath, os.ModePerm); err != nil {
		return err
	}
	isGitRepo, err := exists(path.Join(repoPath, ".git"))
	if err != nil {
		return err
	}
	if isGitRepo {
		return nil
	}
	cmd := execCommand("git", "clone", "--no-checkout", "--depth=1", "--filter=tree:0", "--branch="+cfg.Ref, cfg.Repository, repoPath)
	stdout, err := cmd.Output()
	if err != nil {
		return err
	}
	fmt.Println(string(stdout))
	return nil
}

// applySparseCheckout sets the sparse-checkout patterns derived from cfg and
// checks out the configured ref. The current working directory must already
// be the repo root.
func applySparseCheckout(cfg *GitSourceConfig) error {
	dirs := sparseCheckoutDirs(cfg)
	cmd := execCommand("git", append([]string{"sparse-checkout", "set", "--no-cone"}, dirs...)...)
	stdout, err := cmd.Output()
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, string(stdout))

	cmd = execCommand("git", "checkout", "origin/"+cfg.Ref)
	stdout, err = cmd.Output()
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, string(stdout))
	return nil
}

// sparseCheckoutDirs returns the list of "/foo" patterns to feed to
// `git sparse-checkout set` so that templates, includes, and skills declared
// on cfg are materialized in the working tree. Glob tails on skill paths are
// stripped so the parent directory is included in the sparse-checkout set.
func sparseCheckoutDirs(cfg *GitSourceConfig) []string {
	var dirs []string
	for _, t := range cfg.Templates {
		dirs = append(dirs, "/"+cleanRelDir(path.Dir(t.Path)))
	}
	for _, inc := range cfg.Includes {
		dirs = append(dirs, "/"+cleanRelDir(path.Dir(inc.Path)))
	}
	for _, sk := range cfg.Skills {
		p := strings.TrimSuffix(stripGlobTail(sk.Path), "/")
		if cleaned := cleanRelDir(p); cleaned != "" {
			dirs = append(dirs, "/"+cleaned)
		}
	}
	return dirs
}

// cleanRelDir trims leading "./" and "/" so the result is a repo-relative
// path suitable for use as a sparse-checkout pattern.
func cleanRelDir(p string) string {
	p = strings.TrimPrefix(p, "./")
	p = strings.TrimPrefix(p, "/")
	return p
}

// stripGlobTail returns p truncated at the first wildcard metacharacter so
// that only the fixed directory prefix remains.
func stripGlobTail(p string) string {
	if i := strings.IndexAny(p, "*?["); i >= 0 {
		return p[:i]
	}
	return p
}

// expandTemplateGlobs replaces every Templates entry whose Path contains "*"
// with one entry per matching file under repoPath. Each match's Name is
// derived from the literal prefix of the original glob. cfg.Templates is
// mutated in place; aggregated errors are returned via errors.Join.
func expandTemplateGlobs(cfg *GitSourceConfig, repoPath string) error {
	var expandError []error
	cfg.Templates = lo.FlatMap(cfg.Templates, func(f FileRef, _ int) []FileRef {
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
	return errors.Join(expandError...)
}

// resolveIncludePaths expands every Includes entry's glob against repoPath
// into a flat list of absolute paths.
func resolveIncludePaths(cfg *GitSourceConfig, repoPath string) ([]string, error) {
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

// resolveSkills resolves each Skills entry into zero or more GitSkillResult
// values. A non-existent literal path or a glob with no matches is a hard
// error. A directory without a SKILL.md is silently dropped.
func resolveSkills(cfg *GitSourceConfig, repoPath string) ([]GitSkillResult, error) {
	var out []GitSkillResult
	for _, sk := range cfg.Skills {
		results, err := resolveSkillEntry(cfg, repoPath, sk)
		if err != nil {
			return nil, err
		}
		out = append(out, results...)
	}
	return out, nil
}

// resolveSkillEntry resolves a single Skills entry — a literal directory or
// a glob — into zero or more GitSkillResult values. A missing path or empty
// glob match is a hard error; a non-directory match is a hard error;
// directories that exist but lack a SKILL.md are silently skipped.
func resolveSkillEntry(cfg *GitSourceConfig, repoPath string, sk FileRef) ([]GitSkillResult, error) {
	pattern := filepath.Join(repoPath, sk.Path)
	var matches []string
	if strings.ContainsAny(sk.Path, "*?[") {
		m, err := filepath.Glob(pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to glob skills path %q in repo %s: %w", sk.Path, cfg.Repository, err)
		}
		matches = m
	} else {
		matches = []string{pattern}
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("skills path %q in repo %s did not match any entries", sk.Path, cfg.Repository)
	}

	var out []GitSkillResult
	for _, m := range matches {
		info, err := os.Stat(m)
		if err != nil {
			return nil, fmt.Errorf("skills path %q in repo %s: %w", sk.Path, cfg.Repository, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("skills path %q in repo %s resolved to non-directory %q", sk.Path, cfg.Repository, m)
		}
		// Directories without a SKILL.md are not installable skills; skip.
		if _, err := os.Stat(filepath.Join(m, "SKILL.md")); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("checking SKILL.md in %q: %w", m, err)
		}
		out = append(out, GitSkillResult{Path: m, Git: cfg})
	}
	return out, nil
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
