// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Model fixture type annotated with plumber:derive inplace directives for acceptance testing of inplace field merging.

// Package fixture provides fixture types used to test plumber inplace derive code generation that merges fields into existing structs.
package fixture

import "github.com/getoutreach/plumber/test/acceptance/fixture/complex"

// plumber:derive
// plumber:mode inplace
// plumber:name ModelBlended
type Model struct {
	// Name
	//
	// is:filtrable
	Name string

	Concurrency int

	// Closer
	//
	// is:filtrable
	Closer OpenCloser

	Queues []string

	Complex complex.Complex
}
