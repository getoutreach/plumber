// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Annotable view type which wraps a slice of annotations for use in render templates.

package view

import "github.com/getoutreach/plumber/query/model"

type Annotable struct {
	Annotations []model.Annotation
}

func (a Annotable) GetAnnotations() model.Annotations {
	return a.Annotations
}
