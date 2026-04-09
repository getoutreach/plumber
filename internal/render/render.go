package render

import (
	"embed"
	"path"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/query/model"
)

var (
	//go:embed templates/*
	EmbededTemplates embed.FS
)

type TypeWrapperProvider interface {
	WrapType(name string, t *model.TypeSpec) (*model.TypeSpec, error)
}

type Template interface {
	Name() string
}

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

func (c Context) WithIgnores(ignores *Ignores) Context {
	return Context{
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

func (c Context) ContextRenderOptions() []gen.RenderOptionsFunc {
	opts := make([]gen.RenderOptionsFunc, 0, len(c.RenderOptions)+1)
	opts = append(opts, c.RenderOptions...)
	if c.Templates != nil {
		opts = append(opts, gen.WithFS(EmbededTemplates,
			c.Templates...,
		))
	}
	return opts
}

type DstOutput struct {
	File    *dst.File
	Package *decorator.Package
}

type Output struct {
	Filename string
	Modules  *ModuleRegister
	Content  []byte
	Dst      *DstOutput
}

func DefaultScope(context Context, scope map[string]any, output string) map[string]any {
	scope["Package"] = map[string]any{
		"Name": path.Base(path.Dir(output)),
		"Path": context.PkgPath,
	}
	scope["Output"] = map[string]any{
		"Path": output,
	}

	return scope
}
