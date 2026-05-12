// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: TypeSpec functions
package model

import (
	"fmt"
	"go/types"

	"github.com/getoutreach/plumber/internal/astx"
)

func NewTypeSpec(fqn *astx.FQN, t types.Type) TypeSpec {
	return TypeSpec{
		TypeKind: buildTypeKind(fqn, t),
		FQN:      fqn.String(),
		Type:     t,
	}
}

func (t TypeSpec) InstanceOf(of string) (bool, error) {
	fqn, err := astx.ParseFQN(t.FQN)
	if err != nil {
		return false, fmt.Errorf("failed to parse FQN %q: %w", fqn, err)
	}
	ofFqn, err := astx.ParseFQN(of)
	if err != nil {
		return false, fmt.Errorf("failed to parse FQN %q: %w", of, err)
	}
	return fqn.InstanceOf(ofFqn), nil
}
