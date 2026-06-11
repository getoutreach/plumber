// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Transformer for Derive command

package shape

import (
	"fmt"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/render"
	"github.com/getoutreach/plumber/internal/genius/gen"
	baserender "github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
)

// RenderTransformer is a concrete implementation of the Transformer interface for rendering types based on existing struct types.
type RenderTransformer struct {
	BasicTransformer
}

// NewRenderTransformer creates a new RenderTransformer with the given position and annotation.
func NewRenderTransformer(n contract.Node, a model.Annotation) *RenderTransformer {
	return &RenderTransformer{
		BasicTransformer: BasicTransformer{
			Position:       n.GetPosition(),
			Name:           "derive",
			AllowedOptions: defaultOptions,
			Options:        a,
		},
	}
}

func (t *RenderTransformer) Render(
	context *render.Context, tp *model.Type, scope baserender.Scope, output string, opener gen.MemoryFileOpener,
) (string, error) {
	if tp.Struct == nil {
		return "", fmt.Errorf("render transformer can only be applied to struct types, got %s", tp.Spec.Kind)
	}
	return render.Derive(context, tp, scope, output, opener)
}
