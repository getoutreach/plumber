// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Transformer for Shape command

package shape

import (
	"fmt"

	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
)

// Shaper is a concrete implementation of the Transformer interface
// for generating new types based on existing struct or interface types.
type Shaper struct {
	BasicTransformer
}

// NewShaper creates a new Shaper transformer with the given position and annotation.
func NewShaper(pos model.Position, a model.Annotation) *Shaper {
	return &Shaper{
		BasicTransformer: BasicTransformer{
			Position:       pos,
			Name:           "shape",
			AllowedOptions: defaultOptions,
			Options:        a,
		},
	}
}

func (t *Shaper) Render(
	context *render.Context, tp *model.Type, scope map[string]any, output string, opener gen.MemoryFileOpener,
) (string, error) {
	if tp.Interface == nil && tp.Struct == nil {
		return "", fmt.Errorf("shape transformer can only be applied to interface or struct types, got %s", tp.Spec.Kind)
	}
	return render.Shape(context, tp, scope, output, opener)
}
