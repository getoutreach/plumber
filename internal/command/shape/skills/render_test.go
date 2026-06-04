// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for skill template rendering and describe helper functions.

package skills

import (
	"bytes"
	"strings"
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape/describe"
)

func sampleContext() TemplateContext {
	return TemplateContext{
		Description: describe.Description{
			Macros: []describe.MacroDescription{{
				Name: "demoMacro",
				Doc:  describe.DocDescription{Description: "demo macro"},
			}},
			Options: []describe.OptionDescription{{
				Name: "demoOption",
				Doc:  describe.DocDescription{Description: "demo option"},
			}},
			Handlers: []describe.HandlerDescription{{
				Name:    "demoHandler",
				Command: "echo",
			}},
		},
		Functions: describe.FunctionsDescription{},
		Structures: describe.StructuresDescription{
			Structures: []describe.StructureFullDescription{{
				Name:     "demoStructure",
				BasePath: "internal",
				Paths: []describe.StructurePathDescription{{
					Name:         "core",
					Usage:        "structure:core",
					RelativePath: "internal/core",
				}},
			}},
		},
	}
}

func TestRenderPassesThroughPlainContent(t *testing.T) {
	in := []byte("# plain markdown\n\n{{ this looks like Go template but is verbatim }}\n")
	out, err := Render("plain.md", in, sampleContext())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, in) {
		t.Errorf("expected pass-through, got %q", out)
	}
}

func TestRenderInjectsDescribeContent(t *testing.T) {
	in := []byte("# Skill\n\n[[ describeMacros ]]\n\n[[ describeOption \"demoOption\" ]]\n")
	out, err := Render("with-template.md", in, sampleContext())
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if !strings.Contains(s, "demoMacro") {
		t.Errorf("expected demoMacro in output, got: %s", s)
	}
	if !strings.Contains(s, "demoOption") {
		t.Errorf("expected demoOption in output, got: %s", s)
	}
}

func TestRenderUnknownMacroReturnsEmpty(t *testing.T) {
	in := []byte("([[ describeMacro \"missing\" ]])")
	out, err := Render("missing.md", in, sampleContext())
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "()" {
		t.Errorf("expected empty injection, got %q", out)
	}
}

func TestRenderInjectsStructures(t *testing.T) {
	in := []byte("[[ describeStructures ]]")
	out, err := Render("structs.md", in, sampleContext())
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, want := range []string{"demoStructure", "### core", "internal/core"} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in output:\n%s", want, s)
		}
	}
}
