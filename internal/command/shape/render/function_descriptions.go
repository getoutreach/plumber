package render

import (
	"text/template"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/expand"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
)

// EvaluationContext is the context passed to template functions during rendering, containing the overall rendering context,
type EvaluationContext struct {
	*Context
}

func FunctionsDescription() (desc contract.FunctionDescriptions, build func(
	context *Context, scope render.Scope, output string) (fm template.FuncMap, dispose func()),
) {
	d := contract.FunctionDescriptors[*EvaluationContext]{
		{
			Description: contract.FunctionDescription{
				Name:        "type_wrap",
				Description: `Wrap a type with additional functionality`,
				Usage:       `{{ type_wrap .Type "WrapperName" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return typesRendererWithWrapper(c.Context.GetPkgPath(), c.Context.GetModules(), c.Context.Wrapper, c.Context.GetPathResolver())
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "ignored",
				Description: `Check if a name is in the list of ignored names.`,
				Usage:       `{{ ignored "name" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return ignored(c.Context.Ignores)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "expand_name",
				Description: `Expand a name using the context's expansion rules.`,
				Usage:       `{{ expand_name "name" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return expand.Name
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "comment",
				Description: `Wrap a given text into comment lines.`,
				Usage:       `{{ comment "This is a long comment \n\nthat needs to be wrapped." }}`,
			},
			Func: func(c *EvaluationContext) any {
				return comment
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "filter_elements",
				Description: `Filter elements based on certain criteria.`,
				Usage:       `{{ filter_elements .Elements "criteria" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return filterElements
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "receiver",
				Description: `Set the current type in the evaluation context.`,
				Usage:       `{{ receiver }}`,
			},
			Func: func(c *EvaluationContext) any {
				return receiver
			},
		},
	}
	return d, func(context *Context, scope render.Scope, output string) (fm template.FuncMap, dispose func()) {
		c := &EvaluationContext{Context: context}
		return d.ToMap(c), d.Dispose(c)
	}
}

func WithRenderFuncMap(context *Context, scope render.Scope, output string) (opt gen.RenderOptionsFunc, dispose func()) {
	_, build := FunctionsDescription()
	fm, dispose := build(context, scope, output)
	return gen.WithFuncMap(fm), dispose
}
