// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Annotable view type which wraps a slice of annotations for use in render templates.

package view

import "github.com/getoutreach/plumber/query/model"

// Annotable represents an entity that can have annotations, such as a struct or interface type in the AST.
type Annotable struct {
	Annotations []model.Annotation
}

// GetAnnotations returns the annotations associated with the Annotable entity, allowing templates to access annotation data.
func (a Annotable) GetAnnotations() model.Annotations {
	return a.Annotations
}
