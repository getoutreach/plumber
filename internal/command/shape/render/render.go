// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the render Context, Output, and ModuleRegister types used to
// coordinate Go type rendering from the plumber query model.

// Package render provides utilities for rendering Go type definitions from the plumber query model into generated source files.
package render

import (
	"embed"

	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
)

// Vars
var (
	// EmbededTemplates is an embedded filesystem containing the default templates for rendering Go code.
	//
	//go:embed templates/*
	EmbededTemplates embed.FS
)

// TypeWrapperProvider is an interface that provides a method for wrapping a model.TypeSpec
// with additional information or transformations during the rendering process.
type TypeWrapperProvider interface {
	WrapType(name string, t *model.TypeSpec) (*model.TypeSpec, error)
}

// Template represents a renderable template with a name and associated rendering logic.
type Template interface {
	Name() string
}

// Context represents the context for rendering a transformation, containing information about the package, modules, output path,
type Context struct {
	render.ContextCloner
	Ignores *Ignores
	Wrapper TypeWrapperProvider
}

func (c *Context) WithIgnores(ignores *Ignores) *Context {
	return &Context{
		ContextCloner: c.ContextCloner.Clone(),
		Ignores:       ignores,
		Wrapper:       c.Wrapper,
	}
}

func (c *Context) Clone() *Context {
	return &Context{
		ContextCloner: c.ContextCloner.Clone(),
		Ignores:       c.Ignores,
		Wrapper:       c.Wrapper,
	}
}
