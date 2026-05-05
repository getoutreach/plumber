package matcher_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/matcher"
	"github.com/getoutreach/plumber/query/model"
	"gotest.tools/v3/assert"
)

type testAnnotationProvider struct {
	annotations model.Annotations
}

func (t *testAnnotationProvider) GetAnnotations() model.Annotations {
	return t.annotations
}

func TestMatchRules_Kind(t *testing.T) {
	rules := []config.MatchRuleConfig{{Rule: "kind:int"}}
	spec := &model.TypeSpec{TypeKind: model.TypeKind{Kind: model.KindInt}, FQN: "int"}
	assert.Assert(t, matcher.MatchRules(rules, spec, nil))
}

func TestMatchRules_KindNoMatch(t *testing.T) {
	rules := []config.MatchRuleConfig{{Rule: "kind:string"}}
	spec := &model.TypeSpec{TypeKind: model.TypeKind{Kind: model.KindInt}, FQN: "int"}
	assert.Assert(t, !matcher.MatchRules(rules, spec, nil))
}

func TestMatchRules_FQN(t *testing.T) {
	rules := []config.MatchRuleConfig{{Rule: `fqn:"time".Time`}}
	spec := &model.TypeSpec{FQN: `"time".Time`}
	assert.Assert(t, matcher.MatchRules(rules, spec, nil))
}

func TestMatchRules_FQNNoMatch(t *testing.T) {
	rules := []config.MatchRuleConfig{{Rule: `fqn:"time".Time`}}
	spec := &model.TypeSpec{FQN: `"time".Duration`}
	assert.Assert(t, !matcher.MatchRules(rules, spec, nil))
}

func TestMatchRules_AnnotationHas(t *testing.T) {
	rules := []config.MatchRuleConfig{{Rule: "annotation.has:is:filtrable"}}
	spec := &model.TypeSpec{TypeKind: model.TypeKind{Kind: model.KindStruct}}
	subject := &testAnnotationProvider{
		annotations: model.Annotations{{Name: "is:filtrable"}},
	}
	assert.Assert(t, matcher.MatchRules(rules, spec, subject))
}

func TestMatchRules_AnnotationHasNoMatch(t *testing.T) {
	rules := []config.MatchRuleConfig{{Rule: "annotation.has:is:filtrable"}}
	spec := &model.TypeSpec{TypeKind: model.TypeKind{Kind: model.KindStruct}}
	subject := &testAnnotationProvider{
		annotations: model.Annotations{{Name: "some:other"}},
	}
	assert.Assert(t, !matcher.MatchRules(rules, spec, subject))
}

func TestMatchRules_AnnotationHasNilSubject(t *testing.T) {
	rules := []config.MatchRuleConfig{{Rule: "annotation.has:is:filtrable"}}
	spec := &model.TypeSpec{TypeKind: model.TypeKind{Kind: model.KindStruct}}
	assert.Assert(t, !matcher.MatchRules(rules, spec, nil))
}

func TestMatchRules_MultipleRulesORSemantics(t *testing.T) {
	rules := []config.MatchRuleConfig{
		{Rule: "kind:string"},
		{Rule: "kind:int"},
	}
	spec := &model.TypeSpec{TypeKind: model.TypeKind{Kind: model.KindInt}, FQN: "int"}
	assert.Assert(t, matcher.MatchRules(rules, spec, nil))
}

func TestMatchRules_EmptyRules(t *testing.T) {
	spec := &model.TypeSpec{TypeKind: model.TypeKind{Kind: model.KindInt}, FQN: "int"}
	assert.Assert(t, !matcher.MatchRules(nil, spec, nil))
}

func TestFindMatcher_Found(t *testing.T) {
	matchers := []config.MatcherConfig{
		{PlumberMatcher: &config.PlumberMatcherConfig{Name: "alpha", Matches: []config.MatchRuleConfig{{Rule: "kind:int"}}}},
		{PlumberMatcher: &config.PlumberMatcherConfig{Name: "beta", Matches: []config.MatchRuleConfig{{Rule: "kind:string"}}}},
	}
	m, ok := matcher.FindMatcher(matchers, "beta")
	assert.Assert(t, ok)
	assert.Equal(t, m.Name, "beta")
	assert.Equal(t, len(m.Matches), 1)
}

func TestFindMatcher_NotFound(t *testing.T) {
	matchers := []config.MatcherConfig{
		{PlumberMatcher: &config.PlumberMatcherConfig{Name: "alpha"}},
	}
	_, ok := matcher.FindMatcher(matchers, "gamma")
	assert.Assert(t, !ok)
}

func TestFindMatcher_NilPlumberMatcher(t *testing.T) {
	matchers := []config.MatcherConfig{
		{PlumberMatcher: nil},
		{PlumberMatcher: &config.PlumberMatcherConfig{Name: "valid"}},
	}
	m, ok := matcher.FindMatcher(matchers, "valid")
	assert.Assert(t, ok)
	assert.Equal(t, m.Name, "valid")
}

func TestResolveRules_InlineMatches(t *testing.T) {
	expr := &config.PlumberWrapperExpressionConfig{
		Matches: []config.MatchRuleConfig{{Rule: "kind:int"}},
	}
	rules, err := matcher.ResolveRules(expr, nil)
	assert.NilError(t, err)
	assert.Equal(t, len(rules), 1)
	assert.Equal(t, rules[0].Rule, "kind:int")
}

func TestResolveRules_NamedMatcher(t *testing.T) {
	expr := &config.PlumberWrapperExpressionConfig{
		Matcher: "myMatcher",
	}
	matchers := []config.MatcherConfig{
		{PlumberMatcher: &config.PlumberMatcherConfig{
			Name:    "myMatcher",
			Matches: []config.MatchRuleConfig{{Rule: "fqn:foo"}, {Rule: "kind:string"}},
		}},
	}
	rules, err := matcher.ResolveRules(expr, matchers)
	assert.NilError(t, err)
	assert.Equal(t, len(rules), 2)
}

func TestResolveRules_NamedMatcherNotFound(t *testing.T) {
	expr := &config.PlumberWrapperExpressionConfig{
		Matcher: "missing",
	}
	_, err := matcher.ResolveRules(expr, nil)
	assert.ErrorContains(t, err, `matcher "missing" not found in config`)
}
