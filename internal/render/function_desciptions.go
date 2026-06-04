// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Function descriptions and builders for template functions.
// nolint: lll //Why: Long lines are used for readability in documentation strings.
package render

import (
	"fmt"
	"html/template"
	"maps"

	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/query/model"
)

// EvaluationContext is the context passed to template functions during rendering, containing the overall rendering context,
type EvaluationContext struct {
	Context Context
	Scope   Scope
	Output  string
	Type    *model.Type
}

func FunctionsDescription() (desc contract.FunctionDescriptions, build func(
	context Context, scope Scope, output string) (fm template.FuncMap, dispose func()),
) {
	d := contract.FunctionDescriptors[*EvaluationContext]{
		{
			Description: contract.FunctionDescription{
				Name: "extend",
				Description: `
                    Extend the current scope with additional variables. It is useful for passing multiple variables to a template without having to create a new
                    struct or map. Usually used in combination with template inclusion for readability and loops to pass loop variable into the included template.
                    `,
				Usage: `
                    {{ with $scope := extend $ "Field1" "Value1" "Field2" "Value2" -}}
                        {{template "plumber/command/shape/struct/field/method" $scope -}}
                    {{ end }}
                    `,
			},
			Func: func(c *EvaluationContext) any {
				return extend
			},
		},
		{
			Description: contract.FunctionDescription{
				Name: "file_description",
				Description: `
                    Sets the description comment for the current file. So second code gen pass that is responsible for file header can pick it up and render it as a comment in the generated code.
                    Example:
                    ` + "```golang" + `

                    // Description: This file contains generated code for MyType and its methods.

                    package sample
                    ` + "```" + `
                    `,
				Usage: `{{ file_description "This file contains generated code for MyType and its methods." }}`,
			},
			Func: func(c *EvaluationContext) any {
				return fileDescription(c.Scope)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name: "file_package_description",
				Description: `
                Sets the description comment for the current file's package.
                Example:
                ` + "```golang" + `

                // Package sample contains generated code for MyType and its methods.
                package sample
                ` + "```" + `
                `,
				Usage: `{{ file_package_description "contains generated code for MyType and its methods." }}`,
			},
			Func: func(c *EvaluationContext) any {
				return filePackageDescription(c.Scope)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name: "comment_wrap",
				Description: `
                    Render text as wrapped comments.

                    Example:
                    ` + "```golang" + `
                    // This is a long comment
                    //
                    // that needs to be wrapped.
                    ` + "```" + `
                `,
				Usage: `{{ comment_wrap "This is a long comment \n\nthat needs to be wrapped." }}`,
			},
			Func: func(c *EvaluationContext) any {
				return commentWrap
			},
		},
		{
			Description: contract.FunctionDescription{
				Name: "type",
				Description: `
                    Render a type by given type specification. It takes into account the imports and aliases defined in the current context to render the type in the most concise way possible.
                `,
				Usage: `{{ type .Type.Spec }}`,
			},
			Func: func(c *EvaluationContext) any {
				return TypesRenderer(c.Context.GetPkgPath(), c.Context.GetModules(), c.Context.GetPathResolver())
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "type_set",
				Description: `Set the current type in the evaluation context. So the methods like type_method_definable can use it.`,
				Usage:       `{{ type_set "MyType" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return typeSet(c.Context, func(t *model.Type) {
					c.Type = t
				})
			},
		},
		{
			Description: contract.FunctionDescription{
				Name: "type_method_definable",
				Description: `
                    Check if a method is undefined or defined within same file as the current output.
					It requires function type_set to set the type first.`,
				Usage: `{{ type_method_definable "MethodName" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return typeMethodUndefined(c.Context, c.Type)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "placeholder",
				Description: `Insert a placeholder in the template so it enables editing within designated area in the output. When generating code with ` + "`inplace`" + ` mode, the placeholder is not rendered.`,
				Usage:       `{{ placeholder "placeholder_name" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return placeholder(c.Scope)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "fragment_start",
				Description: `Renders a start of a fragment. Fragments are similar to placeholders, but allows redefine bigger areas that might contain placeholders.`,
				Usage:       `{{ fragment_start "fragment_name" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return fragmentStart(c.Scope)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "fragment_end",
				Description: `Renderers the end of a fragment.`,
				Usage:       `{{ fragment_end "fragment_name" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return fragmentEnd(c.Scope)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "module_import",
				Description: `Schedules a module for import, so in second pass it will be included in the imports section of the generated file. See module function for more details.`,
				Usage:       `{{ module_import "module_name" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return moduleImport(c.Context)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name: "module",
				Description: `
                Schedules a module for import, so in second pass it will be included in the imports section of the generated file.
                Additionally, it returns a reference to the module that can be used as helper in rendering the module's types.

                It can accept:
                - absolute path like ` + "`github.com/getoutreach/module`" + ` or ` + "`context`" + `
                - relative path like ` + "`../module`" + ` . The relative path is resolved based on the current output path
                - structure path like ` + "`structure:domain.entity`" + ` that resolves to the module containing the specified structure.
                `,
				Usage: `
                {{ $entity      := module "structure:domain.entity" -}}
                {{ $entity.Ident "TypeName" }}
                `,
			},
			Func: func(c *EvaluationContext) any {
				return module(c.Context)
			},
		},
	}
	return d, func(context Context, scope Scope, output string) (fm template.FuncMap, dispose func()) {
		c := &EvaluationContext{
			Context: context,
			Scope:   scope,
			Output:  output,
		}
		return d.ToMap(c), d.Dispose(c)
	}
}

func GenericFunctionsDescription() (desc contract.FunctionDescriptions, funcs template.FuncMap) {
	d := contract.FunctionDescriptors[contract.VoidContext]{
		{
			Description: contract.FunctionDescription{
				Name:        "annotation",
				Description: `Get the annotation with the specified name from an object.`,
				Usage:       `{{ annotation .Type "annotation_name" }}`,
			},
			Func: func(c contract.VoidContext) any {
				return annotation
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "annotation_value",
				Description: `Get the value of an annotation with the specified name from an object.`,
				Usage:       `{{ annotation_value .Type "annotation_name" }}`,
			},
			Func: func(c contract.VoidContext) any {
				return AnnotationValue
			},
		},
		{
			Description: contract.FunctionDescription{
				Name: "fqn_mask",
				Description: `
                Derive new FQN from given and mask that will change the name of the type but keep the same package and import path.
                It is useful for rendering types that are related to each other and should be placed in the same package, like filters, parameters, results etc.

                For example, if you have a type ` + "`User`" + ` with FQN ` + "`github.com/getoutreach/api.User`" + `
                and you want to render a filter type for it, you can use mask ` + "`%s_Filter`" + ` to get FQN ` + "`github.com/getoutreach/api.User_Filter`" + ` for the filter type.

                `,
				Usage: `{{ fqn_mask .Type.Spec "%s_Filter" }}`,
			},
			Func: func(c contract.VoidContext) any {
				return func(spec model.TypeSpec, mask string) (string, error) {
					fqn, err := astx.ParseFQN(spec.FQN)
					if err != nil {
						return "", fmt.Errorf("failed to parse FQN: %w", err)
					}
					return fqn.Mask(mask).String(), nil
				}
			},
		},
	}
	return d, d.ToMap(contract.VoidContext{})
}

func GenericFunctions() template.FuncMap {
	_, fm := GenericFunctionsDescription()
	return fm
}

func WithRenderFuncMap(context Context, scope Scope, output string) (opt gen.RenderOptionsFunc, dispose func()) {
	_, build := FunctionsDescription()
	fm, dispose := build(context, scope, output)
	maps.Copy(fm, GenericFunctions())
	return gen.WithFuncMap(fm), dispose
}
