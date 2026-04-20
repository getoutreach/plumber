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

	// writer := gen.NewWriter(append([]gen.WriterOption{gen.WithFileOpener(opener), func(wc *gen.WriterConfig) {
	// 	wc.Overwrite = true
	// 	wc.WriterOptions = append(wc.WriterOptions, func(s *gen.BlockWriterSettings) {
	// 		s.PlaceholderName = PlaceholderName
	// 	})
	// }}, opts...)...)
	// ctx := gen.NewContext("neco")

	// scope["Modules"] = context.GetModules()

	// scope = DefaultScope(context, scope, output)

	// fm, dispose := WithRenderFuncMap(context, output)
	// defer dispose()

	// features := gen.Features{
	// 	gen.FeatureFunc(func(ctx *gen.Context, wr *gen.Writer) error {
	// 		c := view.Base{
	// 			Scope: scope,
	// 		}
	// 		return ctx.Write(wr, output, func(ctx *gen.Context, w io.Writer) error {
	// 			return gen.RenderContent(ctx, "plumber/file/content", w, c,
	// 				fm,
	// 				gen.WithFS(EmbededTemplates,
	// 					"templates/file.gtpl",
	// 				),
	// 				gen.WithRenderOptions(context.GetRenderOptions()...),
	// 			)
	// 		})
	// 	}),
	// }

	// if err := features.Render(ctx, writer); err != nil {
	// 	return nil, fmt.Errorf("Error during rendering: %w", err)
	// }

	// return &Output{
	// 	Filename: output,
	// 	Modules:  context.GetModules(),
	// 	Content:  opener.Content(output),
	// }, nil
}
