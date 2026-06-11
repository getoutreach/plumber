// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for platform parsing, autodetection, and destination
// path resolution.

package skills

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestParsePlatform(t *testing.T) {
	for _, name := range []string{"agents", "claude", "copilot", "autodetect"} {
		if _, err := ParsePlatform(name); err != nil {
			t.Errorf("ParsePlatform(%q) returned error: %v", name, err)
		}
	}
	if _, err := ParsePlatform("nope"); err == nil {
		t.Error("expected error for unknown platform")
	}
}

func TestDestination(t *testing.T) {
	cases := []struct {
		platform Platform
		want     string
	}{
		{PlatformAgents, filepath.Join("root", "agents", "skills", "demo")},
		{PlatformClaude, filepath.Join("root", ".claude", "skills", "demo")},
		{PlatformCopilot, filepath.Join("root", ".github", "instructions", "demo.instructions.md")},
	}
	for _, tc := range cases {
		got, err := tc.platform.Destination("root", "demo")
		if err != nil {
			t.Fatalf("Destination(%q): %v", tc.platform, err)
		}
		if got != tc.want {
			t.Errorf("Destination(%q) = %q, want %q", tc.platform, got, tc.want)
		}
	}
}

func TestDetectAndResolvePlatforms(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".claude"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".github"), 0o755); err != nil {
		t.Fatal(err)
	}

	got := DetectPlatforms(dir)
	want := []Platform{PlatformClaude, PlatformCopilot}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DetectPlatforms = %v, want %v", got, want)
	}

	resolved, err := ResolvePlatforms(PlatformAutodetect, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(resolved, want) {
		t.Errorf("ResolvePlatforms(autodetect) = %v, want %v", resolved, want)
	}

	if _, err := ResolvePlatforms(PlatformAutodetect, t.TempDir()); err == nil {
		t.Error("expected ResolvePlatforms to fail when no marker dirs exist")
	}

	resolved, err = ResolvePlatforms(PlatformClaude, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(resolved, []Platform{PlatformClaude}) {
		t.Errorf("ResolvePlatforms(claude) = %v", resolved)
	}
}
