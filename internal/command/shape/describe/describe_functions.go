// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the functions describe logic that extracts template function
// descriptions from registered sources and groups them into configurable sections for output formatting.

package describe

import (
	"github.com/getoutreach/plumber/internal/command/shape/contract"
)

// FunctionSectionInput pairs a section title and description with one or more
// function description sources whose functions should be merged into a single section.
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
// and returns a FunctionsDescription ready for formatting.
func BuildFunctions(inputs []FunctionSectionInput) FunctionsDescription {
	sections := make([]FunctionSectionDescription, 0, len(inputs))
	for _, input := range inputs {
		var funcs []FunctionDescription
		for _, src := range input.Sources {
			for _, fd := range src.Descriptions() {
				funcs = append(funcs, FunctionDescription{
					Name: fd.Name,
					Doc: DocDescription{
						Description: fd.Description,
						Usage:       fd.Usage,
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
