// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the macro expansion logic for the shape command, allowing users to define reusable macros
// that expand into multiple annotations on nodes across all packages.
package shape

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/getoutreach/plumber/query/model"
)

// macroTemplateData is the context supplied to text/template when expanding
// macro annotation values. It exposes the triggering annotation's positional
// and named arguments.
type macroTemplateData struct {
	Args      []string
	NamedArgs map[string]string
}

// expandMacros replaces macro annotations with their defined annotation lists on all nodes
// across all packages. This runs before Walk and buildTransformers so that macros can inject
// entry-point annotations like plumber:derive or plumber:shape.
func expandMacros(pkgs []*model.Package, macros []MacroConfig) error {
	macroMap := make(map[string]*PlumberMacroConfig, len(macros))
	for i := range macros {
		if macros[i].PlumberMacro != nil {
			macroMap[macros[i].PlumberMacro.Name] = macros[i].PlumberMacro
		}
	}
	if len(macroMap) == 0 {
		return nil
	}

	for _, pkg := range pkgs {
		for _, typ := range pkg.Types {
			anns, err := expandAnnotations(typ.TypeNode.Annotations, macroMap)
			if err != nil {
				return err
			}
			typ.TypeNode.Annotations = anns
		}
		for _, fun := range pkg.Functions {
			anns, err := expandAnnotations(fun.TypeNode.Annotations, macroMap)
			if err != nil {
				return err
			}
			fun.TypeNode.Annotations = anns
		}
		for _, v := range pkg.Vars {
			anns, err := expandAnnotations(v.TypeNode.Annotations, macroMap)
			if err != nil {
				return err
			}
			v.TypeNode.Annotations = anns
		}
		for _, comment := range pkg.Comments {
			anns, err := expandAnnotations(comment.Annotations, macroMap)
			if err != nil {
				return err
			}
			comment.Annotations = anns
		}
	}
	return nil
}

// expandAnnotations replaces any annotation whose name matches a macro with the macro's
// defined annotations, preserving the order of non-macro annotations. Template expressions
// ({{ .Args }}, {{ .NamedArgs }}) in the macro's annotation Args and NamedArgs values are
// expanded using the triggering annotation's arguments as context.
func expandAnnotations(annotations model.Annotations, macroMap map[string]*PlumberMacroConfig) (model.Annotations, error) {
	var expanded model.Annotations
	for _, ann := range annotations {
		macro, ok := macroMap[ann.Name]
		if !ok {
			expanded = append(expanded, ann)
			continue
		}

		data := macroTemplateData{
			Args:      ann.Args,
			NamedArgs: ann.NamedArgs,
		}

		for _, macroAnn := range macro.Annotations {
			args, err := expandTemplateSlice(macroAnn.Args, data, macroAnn.Name)
			if err != nil {
				return nil, fmt.Errorf("expanding macro %q annotation %q args: %w", ann.Name, macroAnn.Name, err)
			}

			namedArgs, err := expandTemplateMap(macroAnn.NamedArgs, data, macroAnn.Name)
			if err != nil {
				return nil, fmt.Errorf("expanding macro %q annotation %q namedArgs: %w", ann.Name, macroAnn.Name, err)
			}

			a := model.NewAnnotation(macroAnn.Name, args, model.WithNamedArgs(namedArgs))
			expanded = append(expanded, a)
		}
	}
	return expanded, nil
}

// expandTemplateSlice applies template expansion to each element in a string
// slice, returning a new slice with all templates resolved.
func expandTemplateSlice(values []string, data macroTemplateData, context string) ([]string, error) {
	if len(values) == 0 {
		return values, nil
	}
	result := make([]string, len(values))
	for i, v := range values {
		expanded, err := expandTemplateStr(v, data, fmt.Sprintf("%s/args[%d]", context, i))
		if err != nil {
			return nil, err
		}
		result[i] = expanded
	}
	return result, nil
}

// expandTemplateMap applies template expansion to each value in a string map,
// returning a new map with all templates resolved. Keys are not expanded.
func expandTemplateMap(values map[string]string, data macroTemplateData, context string) (map[string]string, error) {
	if len(values) == 0 {
		return values, nil
	}
	result := make(map[string]string, len(values))
	for k, v := range values {
		expanded, err := expandTemplateStr(v, data, fmt.Sprintf("%s/namedArgs[%s]", context, k))
		if err != nil {
			return nil, err
		}
		result[k] = expanded
	}
	return result, nil
}

// expandTemplateStr parses and executes s as a text/template against data.
// If s contains no template delimiters it is returned as-is.
func expandTemplateStr(s string, data macroTemplateData, name string) (string, error) {
	if !strings.Contains(s, "{{") {
		return s, nil
	}

	tmpl, err := template.New(name).Option("missingkey=error").Parse(s)
	if err != nil {
		return "", fmt.Errorf("parsing template %q: %w", s, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("executing template %q: %w", s, err)
	}
	return buf.String(), nil
}
