// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file renders container files from templates for the discovery command.

// Package render provides functionality to render container files from templates for the discovery command.
package render

import (
	"embed"

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
	context render.Context, output string, entryTemplate string, scope render.Scope, opener gen.FileOpener, opts ...gen.RenderOptionsFunc,
) error {
	context.WithPriorityRenderOptions(
		gen.WithFS(EmbededTemplates,
			"templates/command/discovery.gtpl",
			"templates/command/discovery_container.gtpl",
			"templates/command/discovery_application.gtpl",
		),
	)
	context.WithRenderOptions(opts...)

	_, err := render.File(context, entryTemplate, scope, output)
	return err
}
