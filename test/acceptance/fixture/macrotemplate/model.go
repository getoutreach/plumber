// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines a fixture type annotated with a macro that uses template arguments for acceptance testing.

// Package macrotemplate provides fixture types used to test plumber macro template expansion in code generation.
package macrotemplate

// @tderive Widget
type Order struct {
	ID    string
	Total int
}
