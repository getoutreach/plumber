// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the pluggable SkillsFormatter interface and
// formatters for rendering skill listings in JSON, YAML, and Markdown formats.

package skills

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/getoutreach/plumber/internal/command/shape/describe"
	"gopkg.in/yaml.v3"
)

// Formatter renders a slice of SkillInfo into a specific output format.
type Formatter interface {
	Format(skills []SkillInfo) ([]byte, error)
}

// SkillsFormat returns the skills formatter registered under the given name,
// or an error if no formatter with that name exists. The accepted format
// names are shared with the describe package.
func Format(name string) (Formatter, error) {
	switch name {
	case describe.FormatJSON:
		return jsonSkillsFormatter{}, nil
	case describe.FormatYAML:
		return yamlSkillsFormatter{}, nil
	case describe.FormatMD:
		return mdSkillsFormatter{}, nil
	default:
		return nil, fmt.Errorf("unknown format %q; available: %s, %s, %s",
			name, describe.FormatJSON, describe.FormatYAML, describe.FormatMD)
	}
}

// jsonSkillsFormatter renders skills as indented JSON.
type jsonSkillsFormatter struct{}

func (jsonSkillsFormatter) Format(skills []SkillInfo) ([]byte, error) {
	return json.MarshalIndent(skills, "", "  ")
}

// yamlSkillsFormatter renders skills as YAML.
type yamlSkillsFormatter struct{}

func (yamlSkillsFormatter) Format(skills []SkillInfo) ([]byte, error) {
	return yaml.Marshal(skills)
}

// mdSkillsFormatter renders skills as a Markdown table.
type mdSkillsFormatter struct{}

func (mdSkillsFormatter) Format(skills []SkillInfo) ([]byte, error) {
	var b strings.Builder

	b.WriteString("# Skills\n\n")

	if len(skills) == 0 {
		b.WriteString("_No skills available._\n")
		return []byte(b.String()), nil
	}

	b.WriteString("| Name | Origin | Description |\n")
	b.WriteString("|------|--------|-------------|\n")
	for _, s := range skills {
		fmt.Fprintf(&b, "| %s | %s | %s |\n",
			escapeMDTableCell(s.Name),
			escapeMDTableCell(s.Origin),
			escapeMDTableCell(s.Description))
	}

	return []byte(b.String()), nil
}

// escapeMDTableCell escapes pipe characters and newlines for use in markdown
// table cells. Mirrors describe.escapeMDTable but is duplicated here to avoid
// exporting describe internals.
func escapeMDTableCell(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}
