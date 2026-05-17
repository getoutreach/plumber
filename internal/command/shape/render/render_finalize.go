// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the Finalize function that merges rendered content parts and writes the final output file.

package render

import (
	"fmt"
	"strings"

	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
)

func Finalize(
	context *Context, scope render.Scope, parts []string, output string, opener gen.FileOpener, opts ...gen.WriterOption,
) (*render.Output, error) {
	context = context.Clone()

	context.WithPriorityRenderOptions(
		withRenderFuncMap(context, output),
		gen.WithFS(EmbededTemplates,
			"templates/command/command_derive.gtpl",
		),
	)
	o, err := render.Finalize(context, scope, parts, output, opener, opts...)

	if false && strings.Contains(output, "plumber_inplace_helper.go") {
		fmt.Println("-----------------------------")
		fmt.Println(string(o.Content))
		fmt.Println("-----------------------------")
	}

	return o, err
}
