// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Function descriptions and builders for template functions.
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
				Name:        "extend",
				Description: `Extend the current scope with additional variables`,
				Usage:       `{{ extend . "Key1" "value1" "Key2" "value2" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return extend
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "file_description",
				Description: `Sets the description comment for the current file.`,
				Usage:       `{{ file_description "description text" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return fileDescription(c.Scope)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "file_package_description",
				Description: `Sets the description comment for the current file's package.`,
				Usage:       `{{ file_package_description "description text" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return filePackageDescription(c.Scope)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "comment_wrap",
				Description: `Wrap a given text into comment lines.`,
				Usage:       `{{ comment_wrap "This is a long comment \n\nthat needs to be wrapped." }}`,
			},
			Func: func(c *EvaluationContext) any {
				return commentWrap
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "type",
				Description: `Render a type as a string.`,
				Usage:       `{{ type .Type }}`,
			},
			Func: func(c *EvaluationContext) any {
				return TypesRenderer(c.Context.GetPkgPath(), c.Context.GetModules(), c.Context.GetPathResolver())
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "type_set",
				Description: `Set the current type in the evaluation context.`,
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
				Name: "type_method_undefined",
				Description: `Check if a method is undefined or defined within ` +
					`same file as the current output. for the current type. ` +
					`User type_set to set the type first.`,
				Usage: `{{ type_method_undefined "MethodName" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return typeMethodUndefined(c.Context, c.Type)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "placeholder",
				Description: `Insert a placeholder in the template. Usage: {{ placeholder "placeholder_name" }}`,
				Usage:       `{{ placeholder "placeholder_name" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return placeholder(c.Scope)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "fragment_start",
				Description: `Mark the start of a fragment. Usage: {{ fragment_start "fragment_name" }}`,
				Usage:       `{{ fragment_start "fragment_name" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return fragmentStart(c.Scope)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "fragment_end",
				Description: `Mark the end of a fragment. Usage: {{ fragment_end "fragment_name" }}`,
				Usage:       `{{ fragment_end "fragment_name" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return fragmentEnd(c.Scope)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "module_include",
				Description: `Include a module's content. Usage: {{ module_include "module_name" }}`,
				Usage:       `{{ module_include "module_name" }}`,
			},
			Func: func(c *EvaluationContext) any {
				return moduleInclude(c.Context)
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "module",
				Description: `Render a module. Usage: {{ module "module_name" }}`,
				Usage:       `{{ module "module_name" }}`,
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
				Description: `Get the annotation with the specified name from an object. Usage: {{ annotation . "annotation_name" }}`,
				Usage:       `{{ annotation . "annotation_name" }}`,
			},
			Func: func(c contract.VoidContext) any {
				return annotation
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "annotation_value",
				Description: `Get the value of an annotation with the specified name from an object. Usage: {{ annotation_value . "annotation_name" }}`,
				Usage:       `{{ annotation_value . "annotation_name" }}`,
			},
			Func: func(c contract.VoidContext) any {
				return AnnotationValue
			},
		},
		{
			Description: contract.FunctionDescription{
				Name:        "fqn_mask",
				Description: `Mask a fully qualified name (FQN) using a specified mask. Usage: {{ fqn_mask .Type.Spec "mask_pattern" }}`,
				Usage:       `{{ fqn_mask .Type.Spec "mask_pattern" }}`,
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
