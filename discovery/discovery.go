// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file provides dependency resolution
// sentinel types for generated container code.

// Package discovery provides runtime sentinel types used by
// generated container code to detect undeclared dependencies.
package discovery

import (
	"fmt"

	"github.com/getoutreach/plumber"
)

// FailingUndeclaredDependency is a sentinel type used to represent an
// undeclared dependency in the container.
type FailingUndeclaredDependency struct{}

// UndeclaredDependency returns a sentinel value representing an undeclared dependency of type T.
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

func OneOf[T any](...T) T {
	var zero T
	panic("unselected one of dependency of type " + fmt.Sprintf("%T", zero))
}
