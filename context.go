// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains context related functions and struct
package plumber

import "context"

// detachedContext is context detached form original one.
type detachedContext struct {
	context.Context
	valueSource context.Context
}

// Value returns a value from the original context
func (dc *detachedContext) Value(key interface{}) interface{} {
	return dc.valueSource.Value(key)
}

// DetachCancellation detaches the cancellation this should be used only with ContextCloser.
// It returns a new cancel function to not get context leaked.
func DetachCancellation(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(&detachedContext{
		Context:     context.Background(),
		valueSource: ctx,
	})
}
