// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines error types used by the shape command contract.

package contract

import (
	"errors"
	"fmt"
)

// SyntaxError represents a syntax error encountered while parsing generated
// content. It carries both the underlying parse error and the content that
// failed to parse so that callers can display it for debugging.
type SyntaxError struct {
	// Content is the generated source code that could not be parsed.
	Content string
	// Err is the underlying parse error.
	Err error
}

// Error implements the error interface.
func (e *SyntaxError) Error() string {
	return e.Err.Error()
}

// Unwrap returns the underlying error for use with errors.Is/As.
func (e *SyntaxError) Unwrap() error {
	return e.Err
}

// String returns a human-readable representation including both the
// content that failed to parse and the error message.
func (e *SyntaxError) String() string {
	return fmt.Sprintf("syntax error: %s\n\ncontent:\n%s", e.Err, e.Content)
}

// ErrTransformerRender is a sentinel error value indicating a failure during transformer rendering.
var ErrTransformerRender = errors.New("error during transformer rendering")
