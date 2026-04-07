package shape_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/query/model"
	"gotest.tools/v3/assert"
)

func wrapper(name string, t string, matches []shape.MatchRuleConfig) *shape.TypeWrapper {
	return shape.NewTypeWrapper(&shape.ShapeConfig{
		Type: shape.TypeConfig{
			Wrappers: []shape.WrapperConfig{
				{
					PlumberWrapper: &shape.PlumberWrapperConfig{
						Name: name,
						Expressions: []shape.WrapperExpressionConfig{
							{
								PlumberWrapperExpression: &shape.PlumberWrapperExpressionConfig{
									Type:    t,
									Matches: matches,
								},
							},
						},
					},
				},
			},
		},
	})
}

func TestWrapper(t *testing.T) {
	w := wrapper("wrapper", `"github.com/getoutreach/plumber".type`, []shape.MatchRuleConfig{
		{
			Rule: "kind:int",
		},
	})
	tp, err := w.WrapType("wrapper", &model.TypeSpec{
		TypeKind: model.TypeKind{
			Kind: model.KindInt,
		},
		FQN: "int",
	})
	assert.NilError(t, err)

	assert.Equal(t, tp.FQN, `"github.com/getoutreach/plumber".type[int]`)
}
