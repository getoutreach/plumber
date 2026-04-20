// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the Finalize function that merges rendered content parts and writes the final output file.

package render

import (
	"strings"

	"github.com/getoutreach/plumber/internal/genius/gen"
)

func Finalize(
	context Context, scope map[string]any, parts []string, output string, opener gen.FileOpener, opts ...gen.WriterOption,
) (*Output, error) {
	context = context.Context().Clone()
	context.WithPriorityRenderOptions(
		gen.WithFS(EmbededTemplates,
			"templates/file.gtpl",
		),
	)
	scope["Content"] = strings.Join(parts, "\n\n\n")
	return Render(context, "plumber/file/content", scope, output, opener, opts...)
}
