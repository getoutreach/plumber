// Generated file by plumber shape function. DON'T edit manually.
package contract

import (
	"time"
	// <<plumber::Block(imports)>>
	"fmt"
	// <</plumber::Block>>
)

// <<plumber::Block(header)>>
func init() {
	fmt.Println("test")
}

// <</plumber::Block>>

// DerivedWorker is derived from "github.com/getoutreach/plumber/example/contract".Worker.
// You can customize it but some fields may be automatically re-introduced based on the original struct definition.
type DerivedWorker struct {
	Name         string
	Concurrency  int
	CreatedAt    time.Time
	ComplexField OpenCloser
	Queues       []string
}

// WorkerFilter is derived from "github.com/getoutreach/plumber/example/contract".Worker.
// You can customize it but some fields may be automatically re-introduced based on the original struct definition.
type WorkerFilter struct {
	Name         string
	CreatedAt    time.Time
	ComplexField OpenCloser
}

// <<plumber::Block(footer)>>
// <</plumber::Block>>
