// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the pluggable StructuresFormatter
// interface and provides JSON, YAML, and Markdown formatters for the
// structure-describe subcommand.

package describe

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// StructuresFormatter renders a StructuresDescription into a specific output
// format.
type StructuresFormatter interface {
	FormatStructures(desc StructuresDescription) ([]byte, error)
}

// StructuresFormat returns the structures formatter registered under the
// given name. Supported names are: json, yaml, md.
func StructuresFormat(name string) (StructuresFormatter, error) {
	switch name {
	case "json":
		return jsonStructuresFormatter{}, nil
	case "yaml":
		return yamlStructuresFormatter{}, nil
	case "md", "":
		return mdStructuresFormatter{}, nil
	default:
		return nil, fmt.Errorf("unknown structures format %q (supported: md, json, yaml)", name)
	}
}

// jsonStructuresFormatter renders StructuresDescription as indented JSON.
type jsonStructuresFormatter struct{}

func (jsonStructuresFormatter) FormatStructures(desc StructuresDescription) ([]byte, error) {
	return json.MarshalIndent(desc, "", "  ")
}

// yamlStructuresFormatter renders StructuresDescription as YAML.
type yamlStructuresFormatter struct{}

func (yamlStructuresFormatter) FormatStructures(desc StructuresDescription) ([]byte, error) {
	return yaml.Marshal(desc)
}

// mdStructuresFormatter renders StructuresDescription as human-readable Markdown.
type mdStructuresFormatter struct{}

func (mdStructuresFormatter) FormatStructures(desc StructuresDescription) ([]byte, error) {
	var b strings.Builder
	b.WriteString("# Shape Structures\n\n")
	if len(desc.Structures) == 0 {
		b.WriteString("_No structures registered._\n")
		return []byte(b.String()), nil
	}
	for _, s := range desc.Structures {
		writeStructureFull(&b, s)
	}
	return []byte(b.String()), nil
}

// writeStructureFull renders a single structure section, including its paths.
func writeStructureFull(b *strings.Builder, s StructureFullDescription) {
	fmt.Fprintf(b, "## %s\n\n", s.Name)
	if s.Title != "" {
		fmt.Fprintf(b, "%s\n\n", s.Title)
	}
	if s.BasePath != "" {
		fmt.Fprintf(b, "**Base path:** `%s`\n\n", s.BasePath)
	}
	if s.Documentation != "" {
		fmt.Fprintf(b, "**Documentation:**\n\n%s\n\n", s.Documentation)
	}
	if len(s.Paths) == 0 {
		b.WriteString("_No paths defined._\n\n")
		return
	}
	for _, p := range s.Paths {
		writeStructurePath(b, p)
	}
}

// writeStructurePath renders a single named path inside a structure section.
func writeStructurePath(b *strings.Builder, p StructurePathDescription) {
	heading := p.Title
	if heading == "" {
		heading = p.Name
	}
	if heading == "" {
		if p.RelativePath != "" {
			heading = "`" + p.RelativePath + "`"
		} else {
			heading = "_(unnamed)_"
		}
	}
	fmt.Fprintf(b, "### %s\n\n", heading)
	if p.Documentation != "" {
		fmt.Fprintf(b, "%s\n\n", p.Documentation)
	}

	b.WriteString("| Field | Value |\n")
	b.WriteString("|-------|-------|\n")
	if p.Name != "" {
		fmt.Fprintf(b, "| Name | `%s` |\n", p.Name)
	}
	if p.RelativePath != "" {
		fmt.Fprintf(b, "| Relative path | `%s` |\n", p.RelativePath)
	}
	required := "no"
	if p.Required {
		required = "yes"
	}
	fmt.Fprintf(b, "| Required | %s |\n", required)
	if p.PackageDescription != "" {
		fmt.Fprintf(b, "| Package description | %s |\n", escapeMDTable(p.PackageDescription))
	}
	b.WriteString("\n")
}
