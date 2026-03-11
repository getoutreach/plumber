// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Path iterator for loop config hydration

package discovery

import (
	"fmt"

	"github.com/getoutreach/plumber"
)

type FailingUndeclaredDependency struct{}

func UndeclaredDependency[T any](value T) *FailingUndeclaredDependency {
	return &FailingUndeclaredDependency{}
}

func (*FailingUndeclaredDependency) String() string {
	return "undeclared dependency"
}

func (*FailingUndeclaredDependency) Resolved() bool {
	return true
}

func (*FailingUndeclaredDependency) Iterate(func(dep plumber.Dependency) bool) {
	// No dependencies to iterate over
}

func (*FailingUndeclaredDependency) Error() error {
	return fmt.Errorf("undeclared dependency")
}

func Unresolved[T any]() T {
	var zero T
	panic("unresolved dependency of type " + fmt.Sprintf("%T", zero))
}

func Undefined[T any]() T {
	var zero T
	panic("undefined dependency of type " + fmt.Sprintf("%T", zero))
}
