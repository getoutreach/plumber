package view

import "github.com/getoutreach/plumber/query/model"

type Annotable struct {
	Annotations []model.Annotation
}

func (a Annotable) GetAnnotations() model.Annotations {
	return a.Annotations
}
