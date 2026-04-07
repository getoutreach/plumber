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
