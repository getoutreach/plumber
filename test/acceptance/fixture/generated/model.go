package generated

import "github.com/getoutreach/plumber/test/acceptance/fixture/complex"

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
