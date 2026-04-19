// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file renders container files from templates for the discovery command.

// Package render provides functionality to render container files from templates for the discovery command.
package render

import (
	"embed"
	"fmt"
	"io"

	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
)

// Vars
var (
	// EmbededTemplates is an embedded filesystem containing the default templates for rendering Go code.
	//
	//go:embed templates/*
	EmbededTemplates embed.FS
)

func Render(
	output string, entryTemplate string, c map[string]any, opener gen.FileOpener, opts ...gen.RenderOptionsFunc,
) error {
	writer := gen.NewWriter(gen.WithFileOpener(opener), func(wc *gen.WriterConfig) {
		wc.Overwrite = true
		wc.WriterOptions = append(wc.WriterOptions, func(s *gen.BlockWriterSettings) {
			s.PlaceholderName = render.PlaceholderName
		})
	})
	ctx := gen.NewContext("neco")

	funcMap := map[string]any{}

	features := gen.Features{
		gen.FeatureFunc(func(ctx *gen.Context, wr *gen.Writer) error {
			return ctx.Write(wr, output, func(ctx *gen.Context, w io.Writer) error {
				return gen.RenderContent(ctx, entryTemplate, w, c,
					gen.WithFuncMap(funcMap),
					gen.WithFS(EmbededTemplates,
						"templates/command/discovery.gtpl",
						"templates/command/discovery_container.gtpl",
						"templates/command/discovery_application.gtpl",
					),
					gen.WithRenderOptions(opts...),
				)
			})
		}),
	}

	if err := features.Render(ctx, writer); err != nil {
		return fmt.Errorf("Error during rendering: %w", err)
	}

	return nil
}
