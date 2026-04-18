// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the render Context, Output, and ModuleRegister types used to
// coordinate Go type rendering from the plumber query model.

// Package render provides utilities for rendering Go type definitions from the plumber query model into generated source files.
package render

import (
	"embed"
	"path"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/genius/gen"
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
	RenderOptions []gen.RenderOptionsFunc
	Modules       *ModuleRegister
	Ignores       *Ignores
	PkgPath       string
	Package       *model.Package
	Wrapper       TypeWrapperProvider
	Output        string
	Templates     []string
}

func (c *Context) WithIgnores(ignores *Ignores) *Context {
	return &Context{
		RenderOptions: c.RenderOptions,
		Modules:       c.Modules,
		Ignores:       ignores,
		PkgPath:       c.PkgPath,
		Package:       c.Package,
		Wrapper:       c.Wrapper,
		Output:        c.Output,
		Templates:     c.Templates,
	}
}

func (c *Context) ContextRenderOptions() []gen.RenderOptionsFunc {
	opts := make([]gen.RenderOptionsFunc, 0, len(c.RenderOptions)+1)
	opts = append(opts, c.RenderOptions...)
	if c.Templates != nil {
		opts = append(opts, gen.WithFS(EmbededTemplates,
			c.Templates...,
		))
	}
	return opts
}

// DstOutput represents the output of rendering a transformation, containing the generated dst.File and its
// associated package information.
type DstOutput struct {
	File    *dst.File
	Package *decorator.Package
}

// Output represents the result of rendering a transformation, containing the output filename, rendered content,
// and any associated modules or DST output.
type Output struct {
	Filename string
	Modules  *ModuleRegister
	Content  []byte
	Dst      *DstOutput
}

func DefaultScope(context *Context, scope map[string]any, output string) map[string]any {
	scope["Package"] = map[string]any{
		"Name": path.Base(path.Dir(output)),
		"Path": context.PkgPath,
	}
	scope["Output"] = map[string]any{
		"Path": output,
	}

	return scope
}
