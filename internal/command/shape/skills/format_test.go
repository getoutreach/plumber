// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for the SkillsFormatter implementations (md, json, yaml).

package skills

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSkillsFormatUnknownReturnsError(t *testing.T) {
	if _, err := Format("xml"); err == nil {
		t.Fatal("expected error for unknown format")
	}
}

func TestSkillsFormatMDProducesTable(t *testing.T) {
	f, err := Format("md")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.Format([]SkillInfo{
		{Name: "alpha", Origin: "embedded", Description: "first skill"},
		{Name: "beta", Origin: "https://example.com/repo.git", Description: "second | skill\nwith newline"},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if !strings.HasPrefix(s, "# Skills\n\n") {
		t.Errorf("output should start with title heading; got: %q", s)
	}
	if !strings.Contains(s, "| Name | Origin | Description |") {
		t.Errorf("output should contain header row; got: %q", s)
	}
	if !strings.Contains(s, "| alpha | embedded | first skill |") {
		t.Errorf("output should contain alpha row; got: %q", s)
	}
	// pipes escaped, newlines collapsed
	if !strings.Contains(s, `second \| skill with newline`) {
		t.Errorf("output should escape pipes and newlines; got: %q", s)
	}
}

func TestSkillsFormatMDEmpty(t *testing.T) {
	f, err := Format("md")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.Format(nil)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if !strings.Contains(s, "_No skills available._") {
		t.Errorf("expected empty-state notice; got: %q", s)
	}
}

func TestSkillsFormatJSONRoundTrips(t *testing.T) {
	f, err := Format("json")
	if err != nil {
		t.Fatal(err)
	}
	in := []SkillInfo{
		{Name: "alpha", Origin: "embedded", Description: "first", Files: []string{"SKILL.md"}},
	}
	out, err := f.Format(in)
	if err != nil {
		t.Fatal(err)
	}

	var got []SkillInfo
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output not valid JSON: %v\n%s", err, out)
	}
	if len(got) != 1 || got[0].Name != "alpha" || got[0].Origin != "embedded" || got[0].Description != "first" {
		t.Errorf("round-trip mismatch: %+v", got)
	}
	if len(got[0].Files) != 1 || got[0].Files[0] != "SKILL.md" {
		t.Errorf("files round-trip mismatch: %+v", got[0].Files)
	}
}

func TestSkillsFormatJSONOmitsUnexportedSrc(t *testing.T) {
	f, _ := Format("json")
	out, err := f.Format([]SkillInfo{{Name: "a", Origin: "embedded"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "src") {
		t.Errorf("JSON output should not expose unexported src field; got: %s", out)
	}
}

func TestSkillsFormatYAMLRoundTrips(t *testing.T) {
	f, err := Format("yaml")
	if err != nil {
		t.Fatal(err)
	}
	in := []SkillInfo{
		{Name: "alpha", Origin: "embedded", Description: "first", Files: []string{"SKILL.md"}},
	}
	out, err := f.Format(in)
	if err != nil {
		t.Fatal(err)
	}

	var got []SkillInfo
	if err := yaml.Unmarshal(out, &got); err != nil {
		t.Fatalf("output not valid YAML: %v\n%s", err, out)
	}
	if len(got) != 1 || got[0].Name != "alpha" || got[0].Origin != "embedded" || got[0].Description != "first" {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}
