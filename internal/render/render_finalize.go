// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the Finalize function that merges rendered content parts and writes the final output file.

package render

import (
	"fmt"
	"io"
	"strings"

	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render/view"
)

func Finalize(context Context, scope map[string]any, parts []string, output string, opener gen.FileOpener, opts ...gen.WriterOption) (*Output, error) {
	writer := gen.NewWriter(append([]gen.WriterOption{gen.WithFileOpener(opener), func(wc *gen.WriterConfig) {
		wc.Overwrite = true
		wc.WriterOptions = append(wc.WriterOptions, func(s *gen.BlockWriterSettings) {
			s.PlaceholderName = "plumber"
		})
	}}, opts...)...)
	ctx := gen.NewContext("neco")

	scope["Content"] = strings.Join(parts, "\n\n\n")

	scope["Modules"] = context.Modules

	scope = DefaultScope(context, scope, output)

	fm, dispose := withRenderFuncMap(context, output)
	defer dispose()

	features := gen.Features{
		gen.FeatureFunc(func(ctx *gen.Context, wr *gen.Writer) error {
			c := view.Base{
				Scope: scope,
			}
			return ctx.Write(wr, output, func(ctx *gen.Context, w io.Writer) error {
				return gen.RenderContent(ctx, "plumber/command/derive/file/content", w, c,
					fm,
					gen.WithFS(EmbededTemplates,
						"templates/command/command.gtpl",
						"templates/command/command_derive.gtpl",
					),
					gen.WithTemplateFunc(gen.LoadBaseTemplate(
						"templates/new.gtpl",
					)),
				)
			})
		}),
	}

	if err := features.Render(ctx, writer); err != nil {
		return nil, fmt.Errorf("Error during rendering: %w", err)
	}

	return &Output{
		Filename: output,
		Modules:  context.Modules,
		Content:  opener.Content(output),
	}, nil
}
