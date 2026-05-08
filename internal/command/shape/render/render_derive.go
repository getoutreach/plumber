// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the Derive renderer which generates blended/filtered struct variants from annotated source types.

package render

import (
	"github.com/getoutreach/plumber/internal/command/shape/render/view"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
)

func Derive(context *Context, tp *model.Type, scope render.Scope, output string, opener gen.MemoryFileOpener) (string, error) {
	context = context.Clone()
	context.WithPriorityRenderOptions(
		withRenderFuncMap(context, output),
		gen.WithFS(EmbededTemplates,
			"templates/command/command_derive.gtpl",
		),
	)
	context.WithDataFactoryFunc(func(scope render.Scope) any {
		return &view.Struct{
			Type: tp,
			Base: view.Base{
				Scope: scope,
			},
		}
	})
	return contentError(
		render.Render(context, "plumber/command/derive", scope, output, opener),
	)
}
