// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Base and Struct view types used to pass scope and type information into render templates.

// Package view provides view types used to pass structured data into the plumber render templates.
package view

import "github.com/getoutreach/plumber/query/model"

type Base struct {
	Scope map[string]interface{}
}

type Struct struct {
	Base
	Type *model.Type
}
