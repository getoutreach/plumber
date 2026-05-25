package expand

import (
	"text/template"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/query/model"
)

// EvaluationContext is the context passed to template functions during rendering, containing the overall rendering context,
type EvaluationContext struct {
	node                  model.Node
	structurePathResolver contract.StructurePathResolver
	data                  sourceTemplateData
}

func FunctionsDescription() (desc contract.FunctionDescriptions,
	build func(
		node model.Node, structurePathResolver contract.StructurePathResolver, data sourceTemplateData) (fm template.FuncMap, dispose func(),
	),
) {
	d := contract.FunctionDescriptors[*EvaluationContext]{
		{
			Description: contract.FunctionDescription{
				Name:        "filename_suffixed",
				Description: `Append a suffix to a filename.`,
				Usage:       `{{ filename_suffixed "suffix" }}`,
			},
			Func: filenameSuffixed,
		},
		{
			Description: contract.FunctionDescription{
				Name:        "path_join",
				Description: `Join multiple path segments together using the context's path resolver.`,
				Usage:       `{{ path_join "segment1" "segment2" }}`,
			},
			Func: pathJoin,
		},
		{
			Description: contract.FunctionDescription{
				Name:        "macro_defaults_name",
				Description: `Determine a default name for a macro based on the context's source arguments or type information.`,
				Usage:       `{{ macro_defaults_name }}`,
			},
			Func: macroDefaultsName,
		},
	}
	return d, func(node model.Node, structurePathResolver contract.StructurePathResolver, data sourceTemplateData) (fm template.FuncMap, dispose func()) {
		c := &EvaluationContext{node: node, structurePathResolver: structurePathResolver, data: data}
		return d.ToMap(c), d.Dispose(c)
	}
}

func WithRenderFuncMap(node model.Node, structurePathResolver contract.StructurePathResolver, data sourceTemplateData) (fm template.FuncMap, dispose func()) {
	_, build := FunctionsDescription()
	return build(node, structurePathResolver, data)
}
