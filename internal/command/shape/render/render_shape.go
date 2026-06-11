// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the Shape renderer which generates forwarding/wrapper struct definitions from annotated source types.

package render

import (
	"github.com/getoutreach/plumber/internal/command/shape/render/view"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
)

func Shape(context *Context, tp *model.Type, scope render.Scope, output string, opener gen.MemoryFileOpener) (string, error) {
	context = context.Clone()
	context.WithPriorityRenderOptions(
		withRenderFuncMap(context, output),
		gen.WithFS(EmbededTemplates,
			"templates/command/command_shape.gtpl",
			"templates/command/command_shape_interface.gtpl",
			"templates/command/command_shape_struct.gtpl",
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
		render.Render(context, "plumber/command/shape", scope, output, opener),
	)
}
