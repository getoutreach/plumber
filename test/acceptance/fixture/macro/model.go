// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines a fixture type annotated with a macro for acceptance testing of macro expansion.

// Package macro provides fixture types used to test plumber macro expansion in code generation.
package macro

// @derive
type Model struct {
	Name string

	Concurrency int
}
