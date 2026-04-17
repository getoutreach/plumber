// Generated file by plumber shape function. DON'T edit manually.
package contract

import (
	"time"
	// <<plumber::Block(imports)>>
	// <</plumber::Block>>
)

// <<plumber::Block(header)>>
// <</plumber::Block>>
// GetName returns the Name field.
func (r *Worker) GetName() Name {
	return r.Name
}

// SetName sets the Name field.
func (r *Worker) SetName(value Name) {
	r.Name = value
}

// GetConcurrency returns the Concurrency field.
func (r *Worker) GetConcurrency() int {
	return r.Concurrency
}

// SetConcurrency sets the Concurrency field.
func (r *Worker) SetConcurrency(value int) {
	r.Concurrency = value
}

// GetCreatedAt returns the CreatedAt field.
func (r *Worker) GetCreatedAt() time.Time {
	return r.CreatedAt
}

// SetCreatedAt sets the CreatedAt field.
func (r *Worker) SetCreatedAt(value time.Time) {
	r.CreatedAt = value
}

// GetComplexField returns the ComplexField field.
func (r *Worker) GetComplexField() OpenCloser {
	return r.ComplexField
}

// SetComplexField sets the ComplexField field.
func (r *Worker) SetComplexField(value OpenCloser) {
	r.ComplexField = value
}

// GetQueues returns the Queues field.
func (r *Worker) GetQueues() []string {
	return r.Queues
}

// <<plumber::Block(footer)>>
// <</plumber::Block>>
