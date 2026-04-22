// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file define an expansion function for macros and annotations

// Package expand provides functionality for expanding macro annotations
package expand

import (
	"bytes"
	"fmt"
	"html/template"
	"strings"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/query/model"
)

func Name(v string, t *model.Type) any {
	return strings.ReplaceAll(v, "{name}", t.Name)
}

// macroTemplateData is the context supplied to text/template when expanding
// macro annotation values.
//
// The macro-side fields are exposed under .Macro (the triggering annotation's
// positional and named arguments) and the package the macro is being expanded in
// is exposed under .Package (Name and import Path). Templates therefore use
// expressions such as `{{ index .Macro.Args 0 }}` or `{{ .Package.Path }}`.
type macroTemplateData struct {
	Macro   macroAnnotationData
	Package macroPackageData
}

// macroAnnotationData carries the positional and named arguments of the
// annotation that triggered the macro expansion.
type macroAnnotationData struct {
	Args      []string
	NamedArgs map[string]string
}

// macroPackageData exposes the basic identity of the package whose annotations
// are being expanded.
type macroPackageData struct {
	Name string
	Path string
}

// Macros replaces macro annotations with their defined annotation lists on all nodes
// across all packages. This runs before Walk and buildTransformers so that macros can inject
// entry-point annotations like plumber:derive or plumber:shape.
func Macros(pkgs []*model.Package, macros []config.MacroConfig) error {
	macroMap := make(map[string]*config.PlumberMacroConfig, len(macros))
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
			anns, err := expandAnnotations(pkg, typ.TypeNode.Annotations, macroMap)
			if err != nil {
				return err
			}
			typ.TypeNode.Annotations = anns
		}
		for _, fun := range pkg.Functions {
			anns, err := expandAnnotations(pkg, fun.TypeNode.Annotations, macroMap)
			if err != nil {
				return err
			}
			fun.TypeNode.Annotations = anns
		}
		for _, v := range pkg.Vars {
			anns, err := expandAnnotations(pkg, v.TypeNode.Annotations, macroMap)
			if err != nil {
				return err
			}
			v.TypeNode.Annotations = anns
		}
		for _, comment := range pkg.Comments {
			anns, err := expandAnnotations(pkg, comment.Annotations, macroMap)
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
// in the macro's annotation Args and NamedArgs values are expanded using a context that
// exposes:
//   - .Macro.Args      — positional args supplied to the triggering annotation
//   - .Macro.NamedArgs — named args supplied to the triggering annotation
//   - .Package.Name    — name of the package whose annotations are being expanded
//   - .Package.Path    — import path of that package
//
// The pkg argument supplies the .Package fields and may be nil (in which case both
// Package.Name and Package.Path render as empty strings).
func expandAnnotations(
	pkg *model.Package, annotations model.Annotations, macroMap map[string]*config.PlumberMacroConfig,
) (model.Annotations, error) {
	var expanded model.Annotations
	for i := range annotations {
		ann := annotations[i]
		macro, ok := macroMap[ann.Name]
		if !ok {
			expanded = append(expanded, ann)
			continue
		}

		data := macroTemplateData{
			Macro: macroAnnotationData{
				Args:      ann.Args,
				NamedArgs: ann.NamedArgs,
			},
			Package: packageTemplateData(pkg),
		}

		// Capture a stable pointer to the triggering annotation that subsequent
		// expansion outputs can reference via ImpliedBy. We allocate a copy so the
		// referent's lifetime is independent of the input slice's storage.
		trigger := ann
		for _, macroAnn := range macro.Annotations {
			args, err := expandTemplateSlice(macroAnn.Args, data, macroAnn.Name)
			if err != nil {
				return nil, fmt.Errorf("expanding macro %q annotation %q args: %w", ann.Name, macroAnn.Name, err)
			}

			namedArgs, err := expandTemplateMap(macroAnn.NamedArgs, data, macroAnn.Name)
			if err != nil {
				return nil, fmt.Errorf("expanding macro %q annotation %q namedArgs: %w", ann.Name, macroAnn.Name, err)
			}

			a := model.NewAnnotation(
				macroAnn.Name, args,
				model.WithNamedArgs(namedArgs),
				model.WithImpliedBy(&trigger),
			)
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

// packageTemplateData converts a *model.Package into the small struct exposed under
// .Package in macro templates. A nil package yields an empty struct.
func packageTemplateData(pkg *model.Package) macroPackageData {
	if pkg == nil {
		return macroPackageData{}
	}
	return macroPackageData{
		Name: pkg.Name,
		Path: pkg.Path,
	}
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
