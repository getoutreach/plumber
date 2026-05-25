// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the Markdown output formatter for the describe command.

package describe

import (
	"fmt"
	"path"
	"strings"

	"gopkg.in/yaml.v3"
)

// mdFormatter implements the Formatter interface to render the Description in Markdown format.
type mdFormatter struct{}

func (mdFormatter) Format(desc Description) ([]byte, error) {
	var b strings.Builder

	b.WriteString("# Shape Configuration\n\n")

	writeMacros(&b, desc.Macros)
	writeOptions(&b, desc.Options)
	writeHandlers(&b, desc.Handlers)

	return []byte(b.String()), nil
}

func writeMacros(b *strings.Builder, macros []MacroDescription) {
	b.WriteString("## Macros\n\n")
	if len(macros) == 0 {
		b.WriteString("_No macros registered._\n\n")
		return
	}
	for _, m := range macros {
		fmt.Fprintf(b, "### %s\n\n", m.Name)
		writeDoc(b, m.Doc)
		writeMetadata(b, m.Metadata)
		writeStructure(b, m.Structure)
		if m.Schema != nil {
			writeSchemaSection(b, m.Schema)
		}
		if len(m.Options) > 0 {
			b.WriteString("**Options:**\n\n")
			for _, o := range m.Options {
				fmt.Fprintf(b, "- `%s`\n", o)
			}
			b.WriteString("\n")
		}
	}
}

func writeOptions(b *strings.Builder, options []OptionDescription) {
	b.WriteString("## Options\n\n")
	if len(options) == 0 {
		b.WriteString("_No options registered._\n\n")
		return
	}
	for _, o := range options {
		fmt.Fprintf(b, "### %s\n\n", o.Name)
		writeDoc(b, o.Doc)
		writeMetadata(b, o.Metadata)
		writeStructure(b, o.Structure)
		if o.Schema != nil {
			writeSchemaSection(b, o.Schema)
		}
	}
}

func writeHandlers(b *strings.Builder, handlers []HandlerDescription) {
	b.WriteString("## Handlers\n\n")
	if len(handlers) == 0 {
		b.WriteString("_No handlers registered._\n\n")
		return
	}
	for _, h := range handlers {
		fmt.Fprintf(b, "### %s\n\n", h.Name)
		writeDoc(b, h.Doc)
		fmt.Fprintf(b, "**Command:** `%s`\n\n", h.Command)
		if len(h.Args) > 0 {
			b.WriteString("**Args:**\n\n")
			for _, a := range h.Args {
				fmt.Fprintf(b, "- `%s`\n", a)
			}
			b.WriteString("\n")
		}
	}
}

func writeDoc(b *strings.Builder, doc DocDescription) {
	if doc.Description != "" {
		fmt.Fprintf(b, "%s\n\n", doc.Description)
	}
	if doc.Usage != "" {
		fmt.Fprintf(b, "**Usage:** \n\n%s\n\n", doc.Usage)
	}
}

func writeMetadata(b *strings.Builder, meta MetadataDescription) {
	b.WriteString("**Metadata:**\n\n")
	b.WriteString("| Name | Details |\n")
	b.WriteString("|------|---------|\n")
	if meta.Source != nil {
		repoName := path.Base(strings.TrimSuffix(meta.Source.Repository, ".git"))
		ref := meta.Source.Ref
		if ref == "" {
			ref = "main"
		}
		fmt.Fprintf(b, "| Source | [%s](%s) @ `%s` |\n", repoName, meta.Source.Repository, ref)
	}
	fmt.Fprintf(b, "| Singular | %v |\n", meta.Singular)
	if meta.Handler != "" {
		fmt.Fprintf(b, "| Handler | `%s` |\n", meta.Handler)
	}
	b.WriteString("\n")
}

func writeStructure(b *strings.Builder, s *StructureDescription) {
	if s == nil {
		return
	}
	b.WriteString("**Structure:** \n\n")
	b.WriteString("| Name | Path |\n")
	b.WriteString("|------|---------|\n")
	fmt.Fprintf(b, "| %s | `%s` |\n\n", s.Name, s.Path)

}

func writeSchemaSection(b *strings.Builder, schema *SchemaDescription) {
	if schema.Positional != nil {
		writePositionalTable(b, schema.Positional)
	}
	if schema.Named != nil {
		writeNamedTable(b, schema.Named)
	}
	// Render raw schema definitions at the end of the section
	hasPositionalRaw := schema.Positional != nil && schema.Positional.Raw != nil
	hasNamedRaw := schema.Named != nil && schema.Named.Raw != nil
	if hasPositionalRaw || hasNamedRaw {
		b.WriteString("**Schema definition:**\n\n")
		if hasPositionalRaw {
			writeRawSchema(b, "Positional", schema.Positional.Raw)
		}
		if hasNamedRaw {
			writeRawSchema(b, "Named", schema.Named.Raw)
		}
	}
}

func writePositionalTable(b *strings.Builder, ps *PositionalSchema) {
	if len(ps.Items) > 0 {
		b.WriteString("**Positional arguments:**\n\n")
		b.WriteString("| # | Type | Description | Required | Details |\n")
		b.WriteString("|---|------|-------------|----------|---------|\n")
		for _, item := range ps.Items {
			req := "no"
			if item.Required {
				req = "yes"
			}
			fmt.Fprintf(b, "| %s | %s | %s | %s | %s |\n",
				item.Position, item.Type, escapeMDTable(item.Description), req, escapeMDTable(item.Details))
		}
		b.WriteString("\n")
	}
}

func writeNamedTable(b *strings.Builder, ns *NamedSchema) {
	if len(ns.Properties) > 0 {
		b.WriteString("**Named arguments:**\n\n")
		b.WriteString("| Name | Type | Description | Required | Details |\n")
		b.WriteString("|------|------|-------------|----------|---------|\n")
		for _, item := range ns.Properties {
			req := "no"
			if item.Required {
				req = "yes"
			}
			fmt.Fprintf(b, "| %s | %s | %s | %s | %s |\n",
				item.Name, item.Type, escapeMDTable(item.Description), req, escapeMDTable(item.Details))
		}
		b.WriteString("\n")
	}
}

func writeRawSchema(b *strings.Builder, label string, raw interface{}) {
	data, err := yaml.Marshal(raw)
	if err != nil {
		return
	}
	fmt.Fprintf(b, "_%s:_\n\n```yaml\n", label)
	b.Write(data)
	b.WriteString("```\n\n")
}

// escapeMDTable escapes pipe characters and newlines for use in markdown table cells.
func escapeMDTable(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}
