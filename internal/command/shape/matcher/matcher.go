// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements reusable match rule evaluation for type specs and annotation
// subjects, as well as named matcher resolution from configuration.

// Package matcher provides reusable matching logic for evaluating match rules (kind, fqn,
// annotation-based) against Go types. It is used by the wrapper and context subsystems of the
// shape command to determine which types satisfy a given set of rules.
package matcher

import (
	"fmt"
	"strings"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/query/model"
)

// MatchRules evaluates a set of match rules against a type spec and an optional
// annotation subject. Returns true if any rule matches (OR semantics).
//
// Supported rule prefixes:
//   - kind:<kind>          — matches when the type's Kind equals <kind>
//   - fqn:<fqn>            — matches when the type's FQN equals <fqn>
//   - annotation.has:<name> — matches when the subject carries an annotation named <name>
func MatchRules(rules []config.MatchRuleConfig, t *model.TypeSpec, subject model.AnnotationProvider) bool {
	for _, match := range rules {
		rule := strings.TrimSpace(match.Rule)
		switch {
		case strings.HasPrefix(rule, "kind:"):
			if rule == fmt.Sprintf("kind:%s", t.Kind.String()) {
				return true
			}
		case strings.HasPrefix(rule, "fqn:"):
			if rule == fmt.Sprintf("fqn:%s", t.FQN) {
				return true
			}
		case strings.HasPrefix(rule, "annotation.has:"):
			annName := strings.TrimPrefix(rule, "annotation.has:")
			if subject != nil && subject.GetAnnotations().Find(annName) != nil {
				return true
			}
		}
	}
	return false
}

// FindMatcher looks up a named matcher from the provided matcher configs.
// Returns the matcher config and true if found, or nil and false otherwise.
func FindMatcher(matchers []config.MatcherConfig, name string) (*config.PlumberMatcherConfig, bool) {
	for i := range matchers {
		if matchers[i].PlumberMatcher != nil && matchers[i].PlumberMatcher.Name == name {
			return matchers[i].PlumberMatcher, true
		}
	}
	return nil, false
}

// ResolveRules returns the match rules for a wrapper expression. When the
// expression references a named matcher, the matcher's rules are returned;
// otherwise the inline matches are used. The two are mutually exclusive.
func ResolveRules(expr *config.PlumberWrapperExpressionConfig, matchers []config.MatcherConfig) ([]config.MatchRuleConfig, error) {
	if expr.Matcher != "" {
		m, ok := FindMatcher(matchers, expr.Matcher)
		if !ok {
			return nil, fmt.Errorf("matcher %q not found in config", expr.Matcher)
		}
		return m.Matches, nil
	}
	return expr.Matches, nil
}
