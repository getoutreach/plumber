// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains unit tests for the functions describe logic and formatters.

package describe

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
)

// stubFunctionDescriptions is a test helper implementing contract.FunctionDescriptions.
type stubFunctionDescriptions struct {
	descs []contract.FunctionDescription
}

func (s stubFunctionDescriptions) Descriptions() []contract.FunctionDescription {
	return s.descs
}

func TestBuildFunctionsEmpty(t *testing.T) {
	desc := BuildFunctions(nil)
	if len(desc.Sections) != 0 {
		t.Errorf("expected 0 sections, got %d", len(desc.Sections))
	}
}

func TestBuildFunctionsSingleSection(t *testing.T) {
	inputs := []FunctionSectionInput{
		{
			Title:       "Expansion",
			Description: "Expansion functions.",
			Sources: []contract.FunctionDescriptions{
				stubFunctionDescriptions{descs: []contract.FunctionDescription{
					{Name: "func_a", Description: "Does A.", Usage: "{{ func_a }}"},
					{Name: "func_b", Description: "Does B.", Usage: "{{ func_b \"arg\" }}"},
				}},
			},
		},
	}

	desc := BuildFunctions(inputs)

	if len(desc.Sections) != 1 {
		t.Fatalf("expected 1 section, got %d", len(desc.Sections))
	}
	s := desc.Sections[0]
	if s.Title != "Expansion" {
		t.Errorf("expected title 'Expansion', got %q", s.Title)
	}
	if s.Description != "Expansion functions." {
		t.Errorf("expected description 'Expansion functions.', got %q", s.Description)
	}
	if len(s.Functions) != 2 {
		t.Fatalf("expected 2 functions, got %d", len(s.Functions))
	}
	if s.Functions[0].Name != "func_a" {
		t.Errorf("expected function name 'func_a', got %q", s.Functions[0].Name)
	}
	if s.Functions[0].Doc.Description != "Does A." {
		t.Errorf("expected description 'Does A.', got %q", s.Functions[0].Doc.Description)
	}
	if s.Functions[0].Doc.Usage != "{{ func_a }}" {
		t.Errorf("expected usage '{{ func_a }}', got %q", s.Functions[0].Doc.Usage)
	}
}

func TestBuildFunctionsMultipleSources(t *testing.T) {
	inputs := []FunctionSectionInput{
		{
			Title: "Combined",
			Sources: []contract.FunctionDescriptions{
				stubFunctionDescriptions{descs: []contract.FunctionDescription{
					{Name: "from_a", Description: "Source A."},
				}},
				stubFunctionDescriptions{descs: []contract.FunctionDescription{
					{Name: "from_b", Description: "Source B."},
					{Name: "from_c", Description: "Source C."},
				}},
			},
		},
	}

	desc := BuildFunctions(inputs)

	if len(desc.Sections) != 1 {
		t.Fatalf("expected 1 section, got %d", len(desc.Sections))
	}
	if len(desc.Sections[0].Functions) != 3 {
		t.Fatalf("expected 3 functions from merged sources, got %d", len(desc.Sections[0].Functions))
	}
}

func TestBuildFunctionsMultipleSections(t *testing.T) {
	inputs := []FunctionSectionInput{
		{
			Title: "Section 1",
			Sources: []contract.FunctionDescriptions{
				stubFunctionDescriptions{descs: []contract.FunctionDescription{
					{Name: "fn1"},
				}},
			},
		},
		{
			Title: "Section 2",
			Sources: []contract.FunctionDescriptions{
				stubFunctionDescriptions{descs: []contract.FunctionDescription{
					{Name: "fn2"},
					{Name: "fn3"},
				}},
			},
		},
	}

	desc := BuildFunctions(inputs)

	if len(desc.Sections) != 2 {
		t.Fatalf("expected 2 sections, got %d", len(desc.Sections))
	}
	if len(desc.Sections[0].Functions) != 1 {
		t.Errorf("expected 1 function in section 1, got %d", len(desc.Sections[0].Functions))
	}
	if len(desc.Sections[1].Functions) != 2 {
		t.Errorf("expected 2 functions in section 2, got %d", len(desc.Sections[1].Functions))
	}
}

func TestBuildFunctionsEmptySource(t *testing.T) {
	inputs := []FunctionSectionInput{
		{
			Title:   "Empty",
			Sources: []contract.FunctionDescriptions{},
		},
	}

	desc := BuildFunctions(inputs)

	if len(desc.Sections) != 1 {
		t.Fatalf("expected 1 section, got %d", len(desc.Sections))
	}
	if len(desc.Sections[0].Functions) != 0 {
		t.Errorf("expected 0 functions, got %d", len(desc.Sections[0].Functions))
	}
}

func TestFunctionsJSONFormatter(t *testing.T) {
	desc := FunctionsDescription{
		Sections: []FunctionSectionDescription{
			{
				Title: "Test Section",
				Functions: []FunctionDescription{
					{Name: "fn1", Doc: DocDescription{Description: "Does something."}},
				},
			},
		},
	}

	f, err := FunctionsFormat("json")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.FormatFunctions(desc)
	if err != nil {
		t.Fatal(err)
	}

	var parsed FunctionsDescription
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if len(parsed.Sections) != 1 {
		t.Fatalf("expected 1 section, got %d", len(parsed.Sections))
	}
	if parsed.Sections[0].Title != "Test Section" {
		t.Errorf("expected title 'Test Section', got %q", parsed.Sections[0].Title)
	}
	if parsed.Sections[0].Functions[0].Name != "fn1" {
		t.Errorf("expected function name 'fn1', got %q", parsed.Sections[0].Functions[0].Name)
	}
}

func TestFunctionsYAMLFormatter(t *testing.T) {
	desc := FunctionsDescription{
		Sections: []FunctionSectionDescription{
			{
				Title: "YAML Section",
				Functions: []FunctionDescription{
					{Name: "fn1", Doc: DocDescription{Description: "YAML test."}},
				},
			},
		},
	}

	f, err := FunctionsFormat("yaml")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.FormatFunctions(desc)
	if err != nil {
		t.Fatal(err)
	}

	var parsed FunctionsDescription
	if err := yaml.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("output is not valid YAML: %v", err)
	}
	if parsed.Sections[0].Title != "YAML Section" {
		t.Errorf("expected title 'YAML Section', got %q", parsed.Sections[0].Title)
	}
}

func TestFunctionsMDFormatter(t *testing.T) {
	desc := FunctionsDescription{
		Sections: []FunctionSectionDescription{
			{
				Title:       "Annotation Value Expansion",
				Description: "Functions for annotation expansion.",
				Functions: []FunctionDescription{
					{
						Name: "filename_suffixed",
						Doc: DocDescription{
							Description: "Append a suffix to a filename.",
							Usage:       `{{ filename_suffixed "suffix" }}`,
						},
					},
					{
						Name: "path_join",
						Doc: DocDescription{
							Description: "Join path segments.",
						},
					},
				},
			},
			{
				Title: "Empty Section",
			},
		},
	}

	f, err := FunctionsFormat("md")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.FormatFunctions(desc)
	if err != nil {
		t.Fatal(err)
	}

	s := string(out)
	if !strings.Contains(s, "# Shape Functions") {
		t.Error("missing top-level heading")
	}
	if !strings.Contains(s, "## Annotation Value Expansion") {
		t.Error("missing section heading")
	}
	if !strings.Contains(s, "Functions for annotation expansion.") {
		t.Error("missing section description")
	}
	if !strings.Contains(s, "### filename_suffixed") {
		t.Error("missing function heading")
	}
	if !strings.Contains(s, "Append a suffix to a filename.") {
		t.Error("missing function description")
	}
	if !strings.Contains(s, "**Usage:** `{{ filename_suffixed \"suffix\" }}`") {
		t.Error("missing function usage")
	}
	if !strings.Contains(s, "### path_join") {
		t.Error("missing path_join function heading")
	}
	if !strings.Contains(s, "## Empty Section") {
		t.Error("missing empty section heading")
	}
	if !strings.Contains(s, "_No functions registered._") {
		t.Error("missing empty section placeholder")
	}
}

func TestFunctionsFormatUnknown(t *testing.T) {
	_, err := FunctionsFormat("xml")
	if err == nil {
		t.Fatal("expected error for unknown format")
	}
	if !strings.Contains(err.Error(), "unknown format") {
		t.Errorf("unexpected error: %v", err)
	}
}
