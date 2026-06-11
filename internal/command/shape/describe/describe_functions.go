// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the functions describe logic that extracts template function
// descriptions from registered sources and groups them into configurable sections for output formatting.

package describe

import (
	"reflect"
	"strings"

	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
)

// FunctionSectionInput pairs a section title and description with one or more
// function description sources whose functions should be merged into a single section.
//
// Sources may implement contract.FunctionSource to additionally supply runtime
// signature information (parameter and result types). Sources that only
// implement contract.FunctionDescriptions are still accepted; their functions
// will be emitted without param/result type information.
type FunctionSectionInput struct {
	Title       string
	Description string
	Sources     []contract.FunctionDescriptions
}

// FunctionsDescription is the top-level output structure for the functions subcommand,
// containing all function sections grouped by category.
type FunctionsDescription struct {
	Sections []FunctionSectionDescription `json:"sections" yaml:"sections"`
}

// FunctionSectionDescription describes a single category of template functions.
type FunctionSectionDescription struct {
	Title       string                `json:"title" yaml:"title"`
	Description string                `json:"description,omitempty" yaml:"description,omitempty"`
	Functions   []FunctionDescription `json:"functions" yaml:"functions"`
}

// BuildFunctions collects function descriptions from all provided section inputs
// and returns a FunctionsDescription ready for formatting. When a source
// implements contract.FunctionSignaturesProvider, the resulting function
// entries include parameter and result type information rendered as FQNs.
func BuildFunctions(inputs []FunctionSectionInput) FunctionsDescription {
	sections := make([]FunctionSectionDescription, 0, len(inputs))
	for _, input := range inputs {
		var funcs []FunctionDescription
		for _, src := range input.Sources {
			if sigSrc, ok := src.(contract.FunctionSignaturesProvider); ok {
				for _, sig := range sigSrc.Signatures() {
					funcs = append(funcs, functionFromSignature(sig))
				}
				continue
			}
			for _, fd := range src.Descriptions() {
				funcs = append(funcs, FunctionDescription{
					Name: fd.Name,
					Doc: DocDescription{
						Description: normalizeDoc(fd.Description),
						Usage:       normalizeDoc(fd.Usage),
					},
				})
			}
		}
		sections = append(sections, FunctionSectionDescription{
			Title:       input.Title,
			Description: input.Description,
			Functions:   funcs,
		})
	}
	return FunctionsDescription{Sections: sections}
}

// functionFromSignature converts a contract.FunctionSignature into the
// describe-layer FunctionDescription, rendering each reflect.Type into its
// FQN string form. The variadic flag, if present, is attached to the final
// parameter (and its type is unwrapped from the implicit slice).
func functionFromSignature(sig contract.FunctionSignature) FunctionDescription {
	fd := FunctionDescription{
		Name: sig.Description.Name,
		Doc: DocDescription{
			Description: normalizeDoc(sig.Description.Description),
			Usage:       normalizeDoc(sig.Description.Usage),
		},
	}
	if len(sig.ParamTypes) > 0 {
		fd.Params = make([]ParamDescription, len(sig.ParamTypes))
		lastIdx := len(sig.ParamTypes) - 1
		for i, pt := range sig.ParamTypes {
			t := pt
			variadic := false
			if sig.Variadic && i == lastIdx && t != nil && t.Kind() == reflect.Slice {
				t = t.Elem()
				variadic = true
			}
			fd.Params[i] = ParamDescription{
				Type:     fqnString(t),
				Variadic: variadic,
			}
		}
	}
	if len(sig.ResultTypes) > 0 {
		fd.Results = make([]ResultDescription, len(sig.ResultTypes))
		for i, rt := range sig.ResultTypes {
			fd.Results[i] = ResultDescription{Type: fqnString(rt)}
		}
	}
	return fd
}

// fqnString returns the FQN string for a reflect.Type, or an empty string
// when the type is nil.
func fqnString(t reflect.Type) string {
	if t == nil {
		return ""
	}
	return astx.FQNFromReflectType(t).String()
}

// normalizeDoc cleans up doc strings authored as multi-line Go raw string
// literals so they render predictably regardless of source indentation. The
// process is:
//
//  1. Drop a leading whitespace-only line (and its trailing newline) and a
//     trailing whitespace-only line (and its preceding newline).
//  2. Determine the leading-whitespace prefix of the first remaining line and
//     strip that exact prefix from every subsequent line that starts with it.
//     Lines indented further than the first line keep the extra (nested)
//     indentation; whitespace-only lines collapse to empty strings.
//
// Lines with less leading whitespace than the first line are left untouched.
func normalizeDoc(s string) string {
	if s == "" {
		return s
	}
	lines := strings.Split(s, "\n")

	// Step 1: drop leading and trailing whitespace-only lines.
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) == 0 {
		return ""
	}

	// Step 2: determine the leading-whitespace prefix of the first line.
	first := lines[0]
	prefixLen := 0
	for prefixLen < len(first) && (first[prefixLen] == ' ' || first[prefixLen] == '\t') {
		prefixLen++
	}
	prefix := first[:prefixLen]

	// Step 3: strip the prefix from each line that has it; collapse
	// whitespace-only interior lines to empty.
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			lines[i] = ""
			continue
		}
		if prefix != "" && strings.HasPrefix(line, prefix) {
			lines[i] = line[prefixLen:]
		}
	}

	return strings.Join(lines, "\n")
}
