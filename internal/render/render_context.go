// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Render context for managing rendering options and modules.
package render

import (
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/query/model"
)

// Context interface
type (
	// RenderContext is the main context struct used during rendering, containing all necessary information
	// and options for the rendering process.
	//nolint: revive //Why: There is already Context Interface defined
	RenderContext struct {
		RenderOptions []gen.RenderOptionsFunc
		Modules       *ModuleRegister
		PkgPath       string
		Package       *model.Package
		Output        string
		Templates     []string
	}

	// Context defines the interface for accessing rendering context information and options.
	Context interface {
		GetRenderOptions() []gen.RenderOptionsFunc
		GetModules() *ModuleRegister
		GetPkgPath() string
		GetPackage() *model.Package
		GetOutput() string
		GetTemplates() []string
		ContextRenderOptions() []gen.RenderOptionsFunc
		WithPriorityRenderOptions(opts ...gen.RenderOptionsFunc)
		WithRenderOptions(opts ...gen.RenderOptionsFunc)
		Context() ContextCloner
	}

	// ContextCloner extends Context with a Clone method for creating copies of the context.
	ContextCloner interface {
		Context
		Clone() ContextCloner
	}

	// Scope is a simple type alias for a map used to hold arbitrary key-value pairs during rendering
	Scope = map[any]any
)

func NewRenderContext(modules *ModuleRegister, pkg *model.Package, output string) *RenderContext {
	return &RenderContext{
		Modules: modules,
		Package: pkg,
		Output:  output,
		PkgPath: pkg.Path,
	}
}

func (c *RenderContext) GetRenderOptions() []gen.RenderOptionsFunc {
	return c.RenderOptions
}

func (c *RenderContext) WithPriorityRenderOptions(opts ...gen.RenderOptionsFunc) {
	c.RenderOptions = append(append([]gen.RenderOptionsFunc{}, opts...), c.RenderOptions...)
}

func (c *RenderContext) WithRenderOptions(opts ...gen.RenderOptionsFunc) {
	c.RenderOptions = append(c.RenderOptions, opts...)
}

func (c *RenderContext) GetModules() *ModuleRegister {
	return c.Modules
}

func (c *RenderContext) GetPkgPath() string {
	return c.PkgPath
}

func (c *RenderContext) GetPackage() *model.Package {
	return c.Package
}

func (c *RenderContext) GetOutput() string {
	return c.Output
}

func (c *RenderContext) GetTemplates() []string {
	return c.Templates
}

func (c *RenderContext) Context() ContextCloner {
	return c
}

func (c *RenderContext) ContextRenderOptions() []gen.RenderOptionsFunc {
	opts := make([]gen.RenderOptionsFunc, 0, len(c.RenderOptions)+1)
	opts = append(opts, c.RenderOptions...)
	if c.Templates != nil {
		opts = append(opts, gen.WithFS(EmbededTemplates,
			c.Templates...,
		))
	}
	return opts
}

func (c *RenderContext) Clone() ContextCloner {
	return &RenderContext{
		RenderOptions: append([]gen.RenderOptionsFunc{}, c.RenderOptions...),
		Modules:       c.Modules,
		PkgPath:       c.PkgPath,
		Package:       c.Package,
		Output:        c.Output,
		Templates:     append([]string{}, c.Templates...),
	}
}
