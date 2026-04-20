// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the Derive renderer which generates blended/filtered struct variants from annotated source types.

package render

import (
	"fmt"
	"io"

	"github.com/getoutreach/plumber/internal/command/shape/render/view"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
)

// PlaceholderName is the name used in templates to reference the context for rendering.
const PlaceholderName = "plumber"

func Derive(context *Context, tp *model.Type, scope map[string]any, output string, opener gen.MemoryFileOpener) (string, error) {
	writer := gen.NewWriter(gen.WithFileOpener(opener), func(wc *gen.WriterConfig) {
		wc.Overwrite = true
		wc.WriterOptions = append(wc.WriterOptions, func(s *gen.BlockWriterSettings) {
			s.PlaceholderName = PlaceholderName
		})
	})

	ctx := gen.NewContext("neco")

	scope = render.DefaultScope(context, scope, output)

	fm, dispose := render.WithRenderFuncMap(context, output)
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
				return gen.RenderContent(ctx, "plumber/command/derive", w, c,
					fm,
					withRenderFuncMap(context, output),
					render.WithBaseTemplates(),
					gen.WithFS(EmbededTemplates,
						"templates/command/command.gtpl",
						"templates/command/command_derive.gtpl",
					),
					gen.WithRenderOptions(context.GetRenderOptions()...),
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
