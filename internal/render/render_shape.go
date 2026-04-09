// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the Shape renderer which generates forwarding/wrapper struct definitions from annotated source types.

package render

import (
	"fmt"
	"io"

	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render/view"
	"github.com/getoutreach/plumber/query/model"
)

func Shape(context Context, tp *model.Type, scope map[string]any, output string, opener gen.MemoryFileOpener) (string, error) {
	writer := gen.NewWriter(gen.WithFileOpener(opener), func(wc *gen.WriterConfig) {
		wc.Overwrite = true
		wc.WriterOptions = append(wc.WriterOptions, func(s *gen.BlockWriterSettings) {
			s.PlaceholderName = "plumber"
		})
	})

	ctx := gen.NewContext("neco")

	scope = DefaultScope(context, scope, output)

	fm, dispose := withRenderFuncMap(context, output)
	defer dispose()

	features := gen.Features{
		gen.FeatureFunc(func(ctx *gen.Context, wr *gen.Writer) error {
			c := &view.Struct{
				Type: tp,
				Base: view.Base{
					Scope: scope,
				},
			}
			return ctx.Write(wr, output, func(ctx *gen.Context, w io.Writer) error {
				return gen.RenderContent(ctx, "plumber/command/shape", w, c,
					fm,
					gen.WithFS(EmbededTemplates,
						"templates/command/command.gtpl",
						"templates/command/command_shape.gtpl",
						"templates/command/command_shape_interface.gtpl",
					),
					gen.WithRenderOptions(context.RenderOptions...),
				)
			})
		}),
	}

	if err := features.Render(ctx, writer); err != nil {
		return "", fmt.Errorf("Error during rendering: %w", err)
	}

	content := opener.Content(output)

	return string(content), nil
}
