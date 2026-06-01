// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the pluggable FunctionsFormatter interface and formatters
// for rendering FunctionsDescription output in JSON, YAML, and Markdown formats.

package describe

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// FunctionsFormatter renders a FunctionsDescription into a specific output format.
type FunctionsFormatter interface {
	FormatFunctions(desc FunctionsDescription) ([]byte, error)
}

// FunctionsFormat returns the functions formatter registered under the given name,
// or an error if no formatter with that name exists.
func FunctionsFormat(name string) (FunctionsFormatter, error) {
	switch name {
	case "json":
		return jsonFunctionsFormatter{}, nil
	case "yaml":
		return yamlFunctionsFormatter{}, nil
	case "md":
		return mdFunctionsFormatter{}, nil
	default:
		return nil, fmt.Errorf("unknown format %q; available: json, yaml, md", name)
	}
}

// jsonFunctionsFormatter renders FunctionsDescription as indented JSON.
type jsonFunctionsFormatter struct{}

func (jsonFunctionsFormatter) FormatFunctions(desc FunctionsDescription) ([]byte, error) {
	return json.MarshalIndent(desc, "", "  ")
}

// yamlFunctionsFormatter renders FunctionsDescription as YAML.
type yamlFunctionsFormatter struct{}

func (yamlFunctionsFormatter) FormatFunctions(desc FunctionsDescription) ([]byte, error) {
	return yaml.Marshal(desc)
}

// mdFunctionsFormatter renders FunctionsDescription as Markdown.
type mdFunctionsFormatter struct{}

func (mdFunctionsFormatter) FormatFunctions(desc FunctionsDescription) ([]byte, error) {
	var b strings.Builder

	b.WriteString("# Shape Functions\n\n")

	for _, section := range desc.Sections {
		fmt.Fprintf(&b, "## %s\n\n", section.Title)
		if section.Description != "" {
			fmt.Fprintf(&b, "%s\n\n", section.Description)
		}
		if len(section.Functions) == 0 {
			b.WriteString("_No functions registered._\n\n")
			continue
		}
		for _, fn := range section.Functions {
			fmt.Fprintf(&b, "### %s\n\n", fn.Name)
			writeDoc(&b, fn.Doc)
			writeFunctionParams(&b, fn.Params)
			writeFunctionResults(&b, fn.Results)
		}
	}

	return []byte(b.String()), nil
}

// writeFunctionParams renders the input parameters of a function as a
// bulleted list. Each entry shows the FQN of the parameter type. The
// final parameter is prefixed with `...` when the function is variadic.
func writeFunctionParams(b *strings.Builder, params []ParamDescription) {
	if len(params) == 0 {
		return
	}
	b.WriteString("**Parameters:**\n\n")
	for _, p := range params {
		prefix := ""
		if p.Variadic {
			prefix = "..."
		}
		fmt.Fprintf(b, "- `%s%s`\n", prefix, p.Type)
	}
	b.WriteString("\n")
}

// writeFunctionResults renders the result types of a function as a
// bulleted list. Each entry shows the FQN of the result type.
func writeFunctionResults(b *strings.Builder, results []ResultDescription) {
	if len(results) == 0 {
		return
	}
	b.WriteString("**Returns:**\n\n")
	for _, r := range results {
		fmt.Fprintf(b, "- `%s`\n", r.Type)
	}
	b.WriteString("\n")
}
