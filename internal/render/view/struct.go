package view

import "github.com/getoutreach/plumber/query/model"

type Base struct {
	Scope map[string]interface{}
}

type Struct struct {
	Base
	Type *model.Type
}
