// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Base and Struct view types used to pass scope and type information into render templates.

// Package view provides view types used to pass structured data into the plumber render templates.
package view

import (
	"github.com/getoutreach/plumber/query/model"
)

// Base represents the base view containing a scope for variables that can be used in templates.
type Base struct {
	Scope map[any]any
}

// Struct represents a view for rendering a struct type, containing the base scope and the specific type information
// for the struct being rendered.
type Struct struct {
	Base
	Type *model.Type
}
