// Generated file by plumber shape function. DON'T edit manually.
package contract

import (
	"time"
	// <<plumber::Block(imports)>>
	// <</plumber::Block>>
)

// <<plumber::Block(header)>>

// <</plumber::Block>>

// ForwardingCloser is derived from "github.com/getoutreach/plumber/example/contract".Closer.
type ForwardingCloser struct {
}

// DerivedWorker is derived from "github.com/getoutreach/plumber/example/contract".Worker.
type DerivedWorker struct {
	Name         Name
	Concurrency  int
	CreatedAt    time.Time
	ComplexField OpenCloser
	Queues       []string
	// <<plumber::Block(extra-DerivedWorker)>>
	// <</plumber::Block>>

}

// WorkerFilter is derived from "github.com/getoutreach/plumber/example/contract".Worker.
type WorkerFilter struct {
	Name         Filtrable[Name]
	CreatedAt    Filtrable[time.Time]
	ComplexField Filtrable[OpenCloser]
	// <<plumber::Block(extra-WorkerFilter)>>
	// <</plumber::Block>>

}

// <<plumber::Block(footer)>>
// <</plumber::Block>>
