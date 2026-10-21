// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for expandTransformations error handling.

package shape

import (
	"errors"
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	baserender "github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"gotest.tools/v3/assert"
)

// expandStubTransformer is a Shaper whose Expand returns a predefined error.
type expandStubTransformer struct {
	Shaper
	err      error
	expanded *int
}

func (t *expandStubTransformer) Expand(
	*contract.ShapingContext, []*model.Package, model.Node, baserender.Scope, map[string]bool,
) error {
	*t.expanded++
	return t.err
}

func TestExpandTransformationsSkipsFailedAndContinues(t *testing.T) {
	expanded := 0
	stub := func(name string, err error) Transformation {
		return Transformation{
			Node: &model.Type{TypeNode: &model.TypeNode{}},
			Transformer: &expandStubTransformer{
				Shaper:   Shaper{BasicTransformer: BasicTransformer{Name: name}},
				err:      err,
				expanded: &expanded,
			},
		}
	}
	transformations := []Transformation{
		stub("ok1", nil),
		stub("broken", errors.New("boom")),
		stub("ok2", nil),
	}

	ctx := contract.NewShapingContext(t.Context(), nil, nil, nil)
	result, err := expandTransformations(ctx, &Config{}, nil, transformations)

	assert.Assert(t, errors.Is(err, errTransformationsSkipped))
	assert.ErrorContains(t, err, "broken")
	assert.ErrorContains(t, err, "boom")
	assert.Equal(t, expanded, 3, "all transformations must be expanded")
	assert.Equal(t, len(result), 2)
	assert.Equal(t, result[0].Transformer.GetName(), "ok1")
	assert.Equal(t, result[1].Transformer.GetName(), "ok2")
}
