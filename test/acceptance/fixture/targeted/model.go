// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines a fixture type with no annotations for testing targeted single-type mode.

// Package targeted provides fixture types used to test plumber shape targeted mode.
package targeted

// Worker is a plain struct with no plumber annotations.
// It will be targeted programmatically via the --type flag.
type Worker struct {
	Name        string
	Concurrency int
}
