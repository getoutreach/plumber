// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements TypeWrapper for wrapping Go type specs based on configured wrapper rules and FQN matching.

package shape

import (
	"fmt"
	"strings"

	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

func NewTypeWrapper(cfg *ShapeConfig) *TypeWrapper {
	return &TypeWrapper{cfg: cfg}
}

type TypeWrapper struct {
	cfg *ShapeConfig
}

func (w *TypeWrapper) WrapType(name string, t *model.TypeSpec) (*model.TypeSpec, error) {
	if w.cfg == nil || w.cfg.Type.Wrappers == nil {
		return nil, nil
	}
	wr, ok := lo.Find(w.cfg.Type.Wrappers, func(wrapper WrapperConfig) bool {
		if wrapper.PlumberWrapper != nil && wrapper.PlumberWrapper.Name == name {
			return true
		}
		return false
	})
	if !ok {
		return nil, fmt.Errorf("wrapper with name %s doesn't exist", name)
	}

	for _, expr := range wr.PlumberWrapper.Expressions {
		if expr.PlumberWrapperExpression != nil {
			matches := expr.PlumberWrapperExpression.Matches
			for _, match := range matches {
				rule := strings.TrimSpace(match.Rule)
				switch {
				case rule == fmt.Sprintf("kind:%s", t.Kind.String()):
					return w.wrap(expr.PlumberWrapperExpression.Type, t)
				case rule == fmt.Sprintf("fqn:%s", t.FQN):
					return w.wrap(expr.PlumberWrapperExpression.Type, t)
				}
			}
		}
	}

	return t, nil
}

func (w *TypeWrapper) wrap(tp string, ts *model.TypeSpec) (*model.TypeSpec, error) {
	wrapping, err := astx.ParseFQN(strings.TrimSpace(tp))
	if err != nil {
		return nil, fmt.Errorf("failed to parse wrapper type FQN %q: %w", tp, err)
	}

	current, err := astx.ParseFQN(strings.TrimSpace(ts.FQN))
	if err != nil {
		return nil, fmt.Errorf("failed to parse current type FQN %q: %w", ts.FQN, err)
	}

	return &model.TypeSpec{
		FQN: wrapping.Wrap(current).String(),
	}, nil
}
