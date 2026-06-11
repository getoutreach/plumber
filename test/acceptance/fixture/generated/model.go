// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Model fixture type annotated with plumber:derive directives
// for acceptance testing of generated code output.

// Package generated provides fixture types used to test plumber shape and derive code generation producing separate output files.
package generated

import "github.com/getoutreach/plumber/test/acceptance/fixture/complex"

// Model is a test struct
//
// plumber:derive DerivedModel
// plumber:mixin mixing.model.filtrable
// plumber:output generated.go
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

// plumber:derive ExternallyDerivedModel
// plumber:model github.com/getoutreach/plumber/test/acceptance/fixture/Model
// plumber:output generated.go
