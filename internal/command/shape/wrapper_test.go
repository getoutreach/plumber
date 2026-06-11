package shape_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/query/model"
	"gotest.tools/v3/assert"
)

func wrapper(name, t string, matches []config.MatchRuleConfig) *shape.TypeWrapper {
	return shape.NewTypeWrapper(&shape.Config{
		Type: config.TypeConfig{
			Wrappers: []config.WrapperConfig{
				{
					PlumberWrapper: &config.PlumberWrapperConfig{
						Name: name,
						Expressions: []config.WrapperExpressionConfig{
							{
								PlumberWrapperExpression: &config.PlumberWrapperExpressionConfig{
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

func wrapperWithMatcher(
	wrapperName, wrapperType, matcherRef, matcherName string, matcherRules []config.MatchRuleConfig,
) *shape.TypeWrapper {
	return shape.NewTypeWrapper(&shape.Config{
		Matchers: []config.MatcherConfig{
			{
				PlumberMatcher: &config.PlumberMatcherConfig{
					Name:    matcherName,
					Matches: matcherRules,
				},
			},
		},
		Type: config.TypeConfig{
			Wrappers: []config.WrapperConfig{
				{
					PlumberWrapper: &config.PlumberWrapperConfig{
						Name: wrapperName,
						Expressions: []config.WrapperExpressionConfig{
							{
								PlumberWrapperExpression: &config.PlumberWrapperExpressionConfig{
									Type:    wrapperType,
									Matcher: matcherRef,
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
	w := wrapper("wrapper", `"github.com/getoutreach/plumber".type`, []config.MatchRuleConfig{
		{
			Rule: "kind:int",
		},
	})
	tp, err := w.WrapType("wrapper", &model.TypeSpec{
		TypeKind: model.TypeKind{
			Kind: model.KindInt,
		},
		FQN: "int",
	}, nil)
	assert.NilError(t, err)

	assert.Equal(t, tp.FQN, `"github.com/getoutreach/plumber".type[int]`)
}

func TestWrapperWithNamedMatcher(t *testing.T) {
	w := wrapperWithMatcher(
		"model.filter",
		`"github.com/getoutreach/plumber".Filtrable`,
		"filtrable",
		"filtrable",
		[]config.MatchRuleConfig{
			{Rule: "kind:int"},
		},
	)
	tp, err := w.WrapType("model.filter", &model.TypeSpec{
		TypeKind: model.TypeKind{
			Kind: model.KindInt,
		},
		FQN: "int",
	}, nil)
	assert.NilError(t, err)
	assert.Equal(t, tp.FQN, `"github.com/getoutreach/plumber".Filtrable[int]`)
}

func TestWrapperWithNamedMatcherNotFound(t *testing.T) {
	w := wrapperWithMatcher(
		"model.filter",
		`"github.com/getoutreach/plumber".Filtrable`,
		"nonexistent",
		"filtrable",
		[]config.MatchRuleConfig{
			{Rule: "kind:int"},
		},
	)
	_, err := w.WrapType("model.filter", &model.TypeSpec{
		TypeKind: model.TypeKind{
			Kind: model.KindInt,
		},
		FQN: "int",
	}, nil)
	assert.ErrorContains(t, err, `matcher "nonexistent" not found in config`)
}

type testAnnotationProvider struct {
	annotations model.Annotations
}

func (t *testAnnotationProvider) GetAnnotations() model.Annotations {
	return t.annotations
}

func TestWrapperAnnotationHasMatch(t *testing.T) {
	w := wrapper("wrapper", `"github.com/getoutreach/plumber".Filtrable`, []config.MatchRuleConfig{
		{Rule: "annotation.has:is:filtrable"},
	})

	subject := &testAnnotationProvider{
		annotations: model.Annotations{
			{Name: "is:filtrable"},
		},
	}

	tp, err := w.WrapType("wrapper", &model.TypeSpec{
		TypeKind: model.TypeKind{Kind: model.KindString},
		FQN:      "string",
	}, subject)
	assert.NilError(t, err)
	assert.Equal(t, tp.FQN, `"github.com/getoutreach/plumber".Filtrable[string]`)
}

func TestWrapperAnnotationHasNoMatch(t *testing.T) {
	w := wrapper("wrapper", `"github.com/getoutreach/plumber".Filtrable`, []config.MatchRuleConfig{
		{Rule: "annotation.has:is:filtrable"},
	})

	subject := &testAnnotationProvider{
		annotations: model.Annotations{
			{Name: "some:other"},
		},
	}

	tp, err := w.WrapType("wrapper", &model.TypeSpec{
		TypeKind: model.TypeKind{Kind: model.KindString},
		FQN:      "string",
	}, subject)
	assert.NilError(t, err)
	// No match, so original type is returned unchanged
	assert.Equal(t, tp.FQN, "string")
}

func TestWrapperAnnotationHasWithNamedMatcher(t *testing.T) {
	w := wrapperWithMatcher(
		"model.filter",
		`"github.com/getoutreach/plumber".Filtrable`,
		"filtrable",
		"filtrable",
		[]config.MatchRuleConfig{
			{Rule: "annotation.has:is:filtrable"},
		},
	)

	subject := &testAnnotationProvider{
		annotations: model.Annotations{
			{Name: "is:filtrable"},
		},
	}

	tp, err := w.WrapType("model.filter", &model.TypeSpec{
		TypeKind: model.TypeKind{Kind: model.KindString},
		FQN:      "string",
	}, subject)
	assert.NilError(t, err)
	assert.Equal(t, tp.FQN, `"github.com/getoutreach/plumber".Filtrable[string]`)
}
