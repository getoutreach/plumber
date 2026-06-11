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
	// "or-plumber-coding-standards" is the directory name, but the SKILL.md
	// frontmatter renames it to "outreach-coding-standards"; the latter is
	// what callers use to select the skill.
	skill := "outreach-coding-standards"
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

// writeExternalSkill creates an external skill directory at dir with a
// SKILL.md whose frontmatter `name` matches the directory basename and
// `description` is the given desc, plus an extra-file with body content.
func writeExternalSkill(t *testing.T, dir, name, desc string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	skillMD := []byte("---\nname: " + name + "\ndescription: " + desc + "\n---\n\n# " + name + "\n\nbody\n")
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), skillMD, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "extra.md"), []byte("# extra\n\nmore\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestListAvailableSkillsIncludesExternal(t *testing.T) {
	tmp := t.TempDir()
	skillDir := filepath.Join(tmp, "ext-skill-1")
	writeExternalSkill(t, skillDir, "ext-skill-1", "ext desc")

	infos, err := ListAvailableSkills([]ExternalSource{
		{Dir: skillDir, Origin: "git@example.com/repo.git"},
	})
	if err != nil {
		t.Fatal(err)
	}

	var got *SkillInfo
	for i := range infos {
		if infos[i].Name == "ext-skill-1" {
			got = &infos[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("external skill not found in listing")
	}
	if got.Origin != "git@example.com/repo.git" {
		t.Errorf("Origin = %q, want git@example.com/repo.git", got.Origin)
	}
	if got.Description != "ext desc" {
		t.Errorf("Description = %q, want %q", got.Description, "ext desc")
	}
	if len(got.Files) != 2 || got.Files[0] != "SKILL.md" {
		t.Errorf("Files = %v, want [SKILL.md, extra.md]", got.Files)
	}
}

func TestListAvailableSkillsExternalOverridesEmbedded(t *testing.T) {
	tmp := t.TempDir()
	// Use a known embedded skill name so the external one shadows it.
	const target = "or-plumber-shape"
	skillDir := filepath.Join(tmp, target)
	writeExternalSkill(t, skillDir, target, "external override")

	infos, err := ListAvailableSkills([]ExternalSource{
		{Dir: skillDir, Origin: "external"},
	})
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	var got *SkillInfo
	for i := range infos {
		if infos[i].Name == target {
			count++
			got = &infos[i]
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one entry for %q, got %d", target, count)
	}
	if got.Origin != "external" {
		t.Errorf("override Origin = %q, want external", got.Origin)
	}
	if got.Description != "external override" {
		t.Errorf("override Description = %q, want %q", got.Description, "external override")
	}
}

func TestInstallExternalFolderPlatform(t *testing.T) {
	tmp := t.TempDir()
	dest := t.TempDir()
	skillDir := filepath.Join(tmp, "ext-folder-skill")
	writeExternalSkill(t, skillDir, "ext-folder-skill", "folder install")

	results, err := Install(InstallOptions{
		Platforms: []Platform{PlatformClaude},
		Skills:    []string{"ext-folder-skill"},
		External:  []ExternalSource{{Dir: skillDir, Origin: "external"}},
		DestRoot:  dest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	expectedDir := filepath.Join(dest, ".claude", "skills", "ext-folder-skill")
	if r.Destination != expectedDir {
		t.Errorf("Destination = %q, want %q", r.Destination, expectedDir)
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
	// SKILL.md and extra.md should both have landed.
	if _, err := os.Stat(filepath.Join(expectedDir, "SKILL.md")); err != nil {
		t.Errorf("missing SKILL.md: %v", err)
	}
	if _, err := os.Stat(filepath.Join(expectedDir, "extra.md")); err != nil {
		t.Errorf("missing extra.md: %v", err)
	}
}

func TestInstallExternalSingleFilePlatform(t *testing.T) {
	tmp := t.TempDir()
	dest := t.TempDir()
	skillDir := filepath.Join(tmp, "ext-single-skill")
	writeExternalSkill(t, skillDir, "ext-single-skill", "single install")

	results, err := Install(InstallOptions{
		Platforms: []Platform{PlatformCopilot},
		Skills:    []string{"ext-single-skill"},
		External:  []ExternalSource{{Dir: skillDir, Origin: "external"}},
		DestRoot:  dest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	expected := filepath.Join(dest, ".github", "instructions", "ext-single-skill.instructions.md")
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
	if !strings.Contains(s, "<!-- source: extra.md -->") {
		t.Error("expected merged content to include extra.md section header")
	}
}

func TestListAvailableSkillsErrorsOnMissingExternalDir(t *testing.T) {
	_, err := ListAvailableSkills([]ExternalSource{
		{Dir: filepath.Join(t.TempDir(), "does-not-exist"), Origin: "x"},
	})
	if err == nil {
		t.Fatal("expected error for missing external skill dir")
	}
}

func TestListAvailableSkillsErrorsOnMissingSkillMD(t *testing.T) {
	tmp := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmp, "no-skill-md"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := ListAvailableSkills([]ExternalSource{
		{Dir: filepath.Join(tmp, "no-skill-md"), Origin: "x"},
	})
	if err == nil {
		t.Fatal("expected error for external dir without SKILL.md")
	}
}
