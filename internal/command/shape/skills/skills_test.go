// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for skill listing and installation onto each supported
// platform layout.

package skills

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestListSkillsReturnsEmbeddedSkills(t *testing.T) {
	infos, err := ListSkills()
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) == 0 {
		t.Fatal("expected at least one embedded skill")
	}
	for _, info := range infos {
		if info.Name == "" {
			t.Errorf("skill has empty name: %+v", info)
		}
		if len(info.Files) == 0 {
			t.Errorf("skill %q has no files", info.Name)
		}
		if info.Files[0] != "SKILL.md" {
			t.Errorf("skill %q first file = %q, want SKILL.md", info.Name, info.Files[0])
		}
	}
}

func TestInstallFolderPlatformWritesAllFiles(t *testing.T) {
	dest := t.TempDir()
	results, err := Install(InstallOptions{
		Platforms: []Platform{PlatformClaude},
		DestRoot:  dest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("expected install results")
	}
	for _, r := range results {
		expected := filepath.Join(dest, ".claude", "skills", r.Skill)
		if r.Destination != expected {
			t.Errorf("Destination = %q, want %q", r.Destination, expected)
		}
		for _, f := range r.Files {
			if f.Skipped {
				t.Errorf("unexpected skip for %s: %s", f.Path, f.Reason)
				continue
			}
			if _, err := os.Stat(f.Path); err != nil {
				t.Errorf("expected file %s to exist: %v", f.Path, err)
			}
		}
	}
}

func TestInstallSingleFilePlatformMergesContent(t *testing.T) {
	dest := t.TempDir()
	skill := "or-plumber-shape"
	results, err := Install(InstallOptions{
		Platforms: []Platform{PlatformCopilot},
		Skills:    []string{skill},
		DestRoot:  dest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	expected := filepath.Join(dest, ".github", "instructions", skill+".instructions.md")
	if r.Destination != expected {
		t.Errorf("Destination = %q, want %q", r.Destination, expected)
	}
	data, err := os.ReadFile(expected)
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	if !strings.HasPrefix(s, "---\n") {
		t.Error("expected YAML frontmatter at file start")
	}
	if !strings.Contains(s, "applyTo:") {
		t.Error("expected applyTo key in frontmatter")
	}
	if !strings.Contains(s, "Shape") {
		t.Errorf("expected skill body content, got: %s", s[:min(len(s), 400)])
	}
}

func TestInstallDryRunWritesNothing(t *testing.T) {
	dest := t.TempDir()
	results, err := Install(InstallOptions{
		Platforms: []Platform{PlatformClaude},
		DestRoot:  dest,
		DryRun:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	for _, r := range results {
		for _, f := range r.Files {
			if !f.Skipped {
				t.Errorf("dry-run wrote file %s", f.Path)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(dest, ".claude")); !os.IsNotExist(err) {
		t.Errorf("dry-run created %s/.claude (err=%v)", dest, err)
	}
}

func TestInstallSkipsExistingWithoutForce(t *testing.T) {
	dest := t.TempDir()
	skill := "or-plumber-coding-standards"
	if _, err := Install(InstallOptions{
		Platforms: []Platform{PlatformClaude},
		Skills:    []string{skill},
		DestRoot:  dest,
	}); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dest, ".claude", "skills", skill, "SKILL.md")
	if err := os.WriteFile(target, []byte("user-edit"), 0o644); err != nil {
		t.Fatal(err)
	}
	results, err := Install(InstallOptions{
		Platforms: []Platform{PlatformClaude},
		Skills:    []string{skill},
		DestRoot:  dest,
	})
	if err != nil {
		t.Fatal(err)
	}
	skipped := false
	for _, r := range results {
		for _, f := range r.Files {
			if f.Path == target && f.Skipped {
				skipped = true
			}
		}
	}
	if !skipped {
		t.Error("expected SKILL.md to be skipped without --force")
	}
	data, _ := os.ReadFile(target)
	if string(data) != "user-edit" {
		t.Errorf("file was overwritten, got %q", data)
	}

	// With force, the file is overwritten.
	if _, err := Install(InstallOptions{
		Platforms: []Platform{PlatformClaude},
		Skills:    []string{skill},
		DestRoot:  dest,
		Force:     true,
	}); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(target)
	if string(data) == "user-edit" {
		t.Error("expected --force to overwrite")
	}
}

func TestInstallUnknownSkillErrors(t *testing.T) {
	_, err := Install(InstallOptions{
		Platforms: []Platform{PlatformClaude},
		Skills:    []string{"does-not-exist"},
		DestRoot:  t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected error for unknown skill")
	}
}
