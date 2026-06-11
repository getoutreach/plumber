// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Unit tests for BuildStructures and the StructuresFormatter
// implementations (md/json/yaml).

package describe

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	"gotest.tools/v3/assert"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/structure"
)

var testRepoModule = contract.ModuleInfo{
	Name:           "plumber",
	NormalizedName: "plumber",
	Path:           "github.com/getoutreach/plumber",
	Dir:            "/repo",
}

var testModule = contract.ModuleInfo{
	Name:           "demo",
	NormalizedName: "demo",
	Path:           "github.com/getoutreach/plumber/demo",
	Dir:            "/repo/demo",
}

func sampleStructureCfg() *shape.Config {
	return &shape.Config{
		StructureDefinitions: &config.StructureDefinitions{
			Structures: []config.PlumberStructureConfig{
				{
					Name:          "service",
					Title:         "Service layout",
					Documentation: "service architecture overview",
					Path:          "internal/{{ .Module.NormalizedName }}",
					Paths: []config.StructurePathConfig{
						{Path: config.PlumberStructurePathConfig{
							Name:          "handlers",
							Title:         "HTTP handler package",
							Documentation: "see internal/handlers/README.md",
							Path:          "handlers",
							Required:      true,
							Template:      "plumber/empty",
						}},
						{Path: config.PlumberStructurePathConfig{
							Name:               "models",
							Title:              "Domain models",
							PackageDescription: "models package",
							Documentation:      "doc reference",
							Path:               "models",
							Templates:          []string{"plumber/types"},
						}},
					},
				},
			},
		},
	}
}

func TestBuildStructuresExpandsTemplates(t *testing.T) {
	desc, err := BuildStructures(sampleStructureCfg(), &structure.NoopResolver{}, testRepoModule, testModule)
	assert.NilError(t, err)
	if len(desc.Structures) != 1 {
		t.Fatalf("expected 1 structure, got %d", len(desc.Structures))
	}
	s := desc.Structures[0]
	if s.BasePath != "internal/demo" {
		t.Errorf("BasePath not expanded: got %q want %q", s.BasePath, "internal/demo")
	}
	if s.Paths[0].RelativePath != "internal/demo/handlers" {
		t.Errorf("RelativePath not expanded: got %q", s.Paths[0].RelativePath)
	}
}

func TestBuildStructuresFromDefinitions(t *testing.T) {
	desc, err := BuildStructures(sampleStructureCfg(), &structure.NoopResolver{}, testRepoModule, testModule)
	assert.NilError(t, err)

	s := desc.Structures[0]
	if s.Name != "service" {
		t.Errorf("unexpected structure name: %q", s.Name)
	}
	if len(s.Paths) != 2 {
		t.Fatalf("expected 2 paths, got %d", len(s.Paths))
	}
	h := s.Paths[0]
	if h.Name != "handlers" {
		t.Errorf("Name = %q, want handlers", h.Name)
	}
	if h.Title != "HTTP handler package" {
		t.Errorf("Title = %q", h.Title)
	}
	if h.Documentation != "see internal/handlers/README.md" {
		t.Errorf("Documentation = %q", h.Documentation)
	}
	if h.Usage != "structure:handlers" {
		t.Errorf("Usage = %q", h.Usage)
	}
	if h.RelativePath != "internal/demo/handlers" {
		t.Errorf("RelativePath = %q", h.RelativePath)
	}
	if !h.Required {
		t.Error("expected handlers required")
	}
	m := s.Paths[1]
	if m.Documentation != "doc reference" || m.PackageDescription != "models package" {
		t.Errorf("doc/package fields not propagated: %+v", m)
	}
	if m.Title != "Domain models" {
		t.Errorf("Title = %q", m.Title)
	}
}

func TestBuildStructuresFallsBackToRawStructures(t *testing.T) {
	cfg := &shape.Config{
		Structures: []*config.StructureDefinitionConfig{
			{Structure: config.PlumberStructureConfig{
				Name: "raw", Path: "pkg/{{ .Module.NormalizedName }}",
				Paths: []config.StructurePathConfig{
					{Path: config.PlumberStructurePathConfig{Name: "core", Path: "core"}},
				},
			}},
		},
	}
	desc, err := BuildStructures(cfg, &structure.NoopResolver{}, testRepoModule, testModule)
	assert.NilError(t, err)

	if len(desc.Structures) != 1 || desc.Structures[0].Name != "raw" {
		t.Fatalf("fallback failed: %+v", desc)
	}
	if desc.Structures[0].BasePath != "pkg/demo" {
		t.Errorf("BasePath not expanded on fallback: %q", desc.Structures[0].BasePath)
	}
	if desc.Structures[0].Paths[0].RelativePath != "pkg/demo/core" {
		t.Errorf("RelativePath = %q", desc.Structures[0].Paths[0].RelativePath)
	}
}

func TestBuildStructuresEmpty(t *testing.T) {
	desc, err := BuildStructures(&shape.Config{}, &structure.NoopResolver{}, testRepoModule, testModule)
	assert.NilError(t, err)
	if len(desc.Structures) != 0 {
		t.Errorf("expected 0 structures, got %d", len(desc.Structures))
	}
}

func TestBuildStructuresDoesNotMutateInput(t *testing.T) {
	cfg := sampleStructureCfg()
	originalBase := cfg.StructureDefinitions.Structures[0].Path
	_, err := BuildStructures(cfg, &structure.NoopResolver{}, testRepoModule, testModule)
	assert.NilError(t, err)
	if cfg.StructureDefinitions.Structures[0].Path != originalBase {
		t.Errorf("BuildStructures mutated input config: %q != %q",
			cfg.StructureDefinitions.Structures[0].Path, originalBase)
	}
}

func TestStructuresFormatMarkdown(t *testing.T) {
	formatter, err := StructuresFormat("md")
	if err != nil {
		t.Fatal(err)
	}
	desc, err := BuildStructures(sampleStructureCfg(), &structure.NoopResolver{}, testRepoModule, testModule)
	assert.NilError(t, err)
	out, err := formatter.FormatStructures(desc)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, want := range []string{
		"# Shape Structures",
		"## service",
		"Service layout",
		"service architecture overview",
		"### HTTP handler package",
		"see internal/handlers/README.md",
		"`internal/demo/handlers`",
		"| Name | `handlers` |",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("markdown missing %q\n---\n%s", want, s)
		}
	}
}

func TestStructuresFormatMarkdownEmpty(t *testing.T) {
	formatter, _ := StructuresFormat("md")
	out, err := formatter.FormatStructures(StructuresDescription{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "_No structures registered._") {
		t.Errorf("expected empty marker, got: %s", out)
	}
}

func TestStructuresFormatJSON(t *testing.T) {
	formatter, _ := StructuresFormat("json")
	desc, err := BuildStructures(sampleStructureCfg(), &structure.NoopResolver{}, testRepoModule, testModule)
	assert.NilError(t, err)
	out, err := formatter.FormatStructures(desc)
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip StructuresDescription
	if err := json.Unmarshal(out, &roundtrip); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if len(roundtrip.Structures) != 1 || roundtrip.Structures[0].Name != "service" {
		t.Errorf("roundtrip mismatch: %+v", roundtrip)
	}
	if roundtrip.Structures[0].Paths[0].Documentation == "" {
		t.Error("Documentation missing from JSON")
	}
}

func TestStructuresFormatYAML(t *testing.T) {
	formatter, _ := StructuresFormat("yaml")
	desc, err := BuildStructures(sampleStructureCfg(), &structure.NoopResolver{}, testRepoModule, testModule)
	assert.NilError(t, err)
	out, err := formatter.FormatStructures(desc)
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip StructuresDescription
	if err := yaml.Unmarshal(out, &roundtrip); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if len(roundtrip.Structures) != 1 {
		t.Errorf("roundtrip mismatch: %+v", roundtrip)
	}
}

func TestStructuresFormatUnknown(t *testing.T) {
	if _, err := StructuresFormat("xml"); err == nil {
		t.Error("expected error for unknown format")
	}
}
