// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for checkoutGit's Skills resolution behavior.

package template

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// makeFakeRepo writes a fake checked-out repo under root containing the given
// relative file paths (each non-empty, joined with filepath.Separator). Each
// file is created with empty content unless the path's basename is "SKILL.md",
// in which case it gets minimal frontmatter.
func makeFakeRepo(t *testing.T, root string, paths []string) {
	t.Helper()
	for _, p := range paths {
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		var body []byte
		if filepath.Base(p) == "SKILL.md" {
			body = []byte("---\nname: " + filepath.Base(filepath.Dir(p)) + "\ndescription: stub\n---\n\n# stub\n")
		}
		if err := os.WriteFile(full, body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCheckoutGitSkillsGlobResolvesDirsWithSkillMD(t *testing.T) {
	repo := t.TempDir()
	makeFakeRepo(t, repo, []string{
		"skills/alpha/SKILL.md",
		"skills/beta/SKILL.md",
		"skills/gamma/extra.md", // no SKILL.md → should be silently dropped
	})

	cfg := &GitSourceConfig{
		Repository: "fake/repo",
		Replaced:   repo,
		Skills:     []FileRef{{Path: "skills/*"}},
	}

	_, skills, err := checkoutGit(cfg, "")
	if err != nil {
		t.Fatalf("checkoutGit: %v", err)
	}

	got := map[string]bool{}
	for _, s := range skills {
		got[filepath.Base(s.Path)] = true
		if s.Git != cfg {
			t.Errorf("skill provenance mismatch: got %v, want %v", s.Git, cfg)
		}
	}
	if !got["alpha"] || !got["beta"] {
		t.Errorf("missing expected skills, got: %v", got)
	}
	if got["gamma"] {
		t.Errorf("directory without SKILL.md should not produce a result: %v", got)
	}
}

func TestCheckoutGitSkillsLiteralPathResolves(t *testing.T) {
	repo := t.TempDir()
	makeFakeRepo(t, repo, []string{"agents/skills/alpha/SKILL.md"})

	cfg := &GitSourceConfig{
		Repository: "fake/repo",
		Replaced:   repo,
		Skills:     []FileRef{{Path: "agents/skills/alpha"}},
	}

	_, skills, err := checkoutGit(cfg, "")
	if err != nil {
		t.Fatalf("checkoutGit: %v", err)
	}
	if len(skills) != 1 {
		t.Fatalf("expected 1 skill, got %d", len(skills))
	}
	if filepath.Base(skills[0].Path) != "alpha" {
		t.Errorf("unexpected skill path: %s", skills[0].Path)
	}
}

func TestCheckoutGitSkillsMissingPathHardFails(t *testing.T) {
	repo := t.TempDir()
	cfg := &GitSourceConfig{
		Repository: "fake/repo",
		Replaced:   repo,
		Skills:     []FileRef{{Path: "skills/missing"}},
	}
	_, _, err := checkoutGit(cfg, "")
	if err == nil {
		t.Fatal("expected hard fail on missing path")
	}
	if !strings.Contains(err.Error(), "skills/missing") {
		t.Errorf("error missing offending path: %v", err)
	}
}

func TestCheckoutGitSkillsEmptyGlobHardFails(t *testing.T) {
	repo := t.TempDir()
	if err := os.MkdirAll(filepath.Join(repo, "skills"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &GitSourceConfig{
		Repository: "fake/repo",
		Replaced:   repo,
		Skills:     []FileRef{{Path: "skills/*"}},
	}
	_, _, err := checkoutGit(cfg, "")
	if err == nil {
		t.Fatal("expected hard fail on glob with zero matches")
	}
}

func TestCheckoutGitSkillsNonDirectoryHardFails(t *testing.T) {
	repo := t.TempDir()
	makeFakeRepo(t, repo, []string{"skills/file.md"})
	cfg := &GitSourceConfig{
		Repository: "fake/repo",
		Replaced:   repo,
		Skills:     []FileRef{{Path: "skills/file.md"}},
	}
	_, _, err := checkoutGit(cfg, "")
	if err == nil {
		t.Fatal("expected hard fail when match is not a directory")
	}
}

func TestCheckoutGitNoSkillsReturnsNothing(t *testing.T) {
	repo := t.TempDir()
	cfg := &GitSourceConfig{
		Repository: "fake/repo",
		Replaced:   repo,
	}
	_, skills, err := checkoutGit(cfg, "")
	if err != nil {
		t.Fatalf("checkoutGit: %v", err)
	}
	if len(skills) != 0 {
		t.Errorf("expected no skills, got %d", len(skills))
	}
}
