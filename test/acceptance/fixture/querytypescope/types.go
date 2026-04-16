// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines a Registry type with function-typed fields for acceptance testing of type-scoped plumber:query annotation processing.

// Package querytypescope provides fixture types and variables for acceptance testing of type-scoped plumber:query annotation processing.
package querytypescope

// Registry holds named getter functions for testing type-scoped queries.
type Registry struct {
	// GetAlpha is a getter function for the alpha subsystem.
	GetAlpha func() string
	// GetBeta is a getter function for the beta subsystem.
	GetBeta func() string
	// SetGamma is a setter function that does not match the Get pattern.
	SetGamma func(string)
}
