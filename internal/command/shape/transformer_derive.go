// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Transformer for Derive command

package shape

import (
	"fmt"

	"github.com/getoutreach/plumber/internal/command/shape/render"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/query/model"
)

// DeriveTransformer is a concrete implementation of the Transformer interface for deriving new types based on existing struct types.
type DeriveTransformer struct {
	BasicTransformer
}

// NewDeriveTransformer creates a new DeriveTransformer with the given position and annotation.
func NewDeriveTransformer(pos model.Position, a model.Annotation) *DeriveTransformer {
	return &DeriveTransformer{
		BasicTransformer: BasicTransformer{
			Position:       pos,
			Name:           "derive",
			AllowedOptions: defaultOptions,
			Options:        a,
		},
	}
}

func (t *DeriveTransformer) Render(
	context *render.Context, tp *model.Type, scope map[string]any, output string, opener gen.MemoryFileOpener,
) (string, error) {
	if tp.Struct == nil {
		return "", fmt.Errorf("derive transformer can only be applied to struct types, got %s", tp.Spec.Kind)
	}
	return render.Derive(context, tp, scope, output, opener)
}
