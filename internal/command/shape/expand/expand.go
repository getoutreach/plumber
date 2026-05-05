// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file define an expansion function for macros and annotations

// Package expand provides functionality for expanding macro annotations
package expand

import (
	"bytes"
	"fmt"
	"maps"
	"path"
	"strings"
	"text/template"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
)

func Name(v string, t *model.Type) any {
	return v
}

// sourceTemplateData is the context supplied to text/template when expanding
// implied annotation values (those produced by a macro or mixin).
//
// The triggering annotation's positional and named arguments are exposed under
// .Source, the package the annotation is being expanded in is exposed under
// .Package (Name and import Path), and the AST node currently being processed
// is exposed under .Type (a contract.Node, typically *model.Type or
// *model.CommentGroup) so templates may inspect type metadata such as
// `{{ .Type.GetAnnotations }}` or `{{ .Type.GetPosition.Filename }}`.
type sourceTemplateData struct {
	Name    string
	Source  *sourceAnnotationData
	Package sourcePackageData
	Output  outputTemplateData
	Type    any
}

// sourceAnnotationData carries the positional and named arguments of the
// annotation that triggered the expansion (the macro or mixin invocation).
type sourceAnnotationData struct {
	Args      []string
	NamedArgs map[string]string
}

// sourcePackageData exposes the basic identity of the package whose annotations
// are being expanded.
type sourcePackageData struct {
	Name string
	Path string
}

// outputTemplateData is the template context used to expand the value of the
// plumber:output annotation. It exposes the source-file identity components
// commonly needed for naming generated files:
//
//   - .Filename — the full base filename of the source file (e.g. "model.go").
//   - .Name     — the source filename without extension (e.g. "model").
//   - .Ext      — the source file extension including the leading dot (".go").
//
// In addition, the template environment registers a `suffixed` function that
// produces "<.Name>_<suffix><.Ext>" — the equivalent of the legacy
// `{suffix:<str>}` placeholder. Example: `{{ filename_suffixed "filter" }}` evaluates
// to `model_filter.go` when expanding output for `model.go`.
type outputTemplateData struct {
	Filename string
	Name     string
	Ext      string
	Dir      string
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
// defined annotation list, preserving the order of non-macro annotations. The
// macro's Args and NamedArgs values are intentionally NOT template-expanded here
// — they are appended verbatim and each child annotation records the triggering
// macro annotation via ImpliedBy. Template expansion is deferred to a later stage
// (see TransformerAnnotations) so that both macro- and mixin-implied annotations
// can be expanded uniformly using their ImpliedBy reference as the data source.
//
// The pkg argument is preserved for symmetry and future use but is no longer
// consulted here since no template execution occurs at this stage.
func expandAnnotations(
	pkg *model.Package, annotations model.Annotations, macroMap map[string]*config.PlumberMacroConfig,
) (model.Annotations, error) {
	_ = pkg
	var expanded model.Annotations
	for i := range annotations {
		ann := annotations[i]
		macro, ok := macroMap[ann.Name]
		// let's not filter it out
		expanded = append(expanded, ann)
		if !ok {
			continue
		}

		// Capture a stable pointer to the triggering annotation that subsequent
		// expansion outputs can reference via ImpliedBy. We allocate a copy so the
		// referent's lifetime is independent of the input slice's storage.
		trigger := ann
		for _, macroAnn := range macro.Annotations {
			a := model.NewAnnotation(
				macroAnn.Name, macroAnn.Args,
				model.WithNamedArgs(macroAnn.NamedArgs),
				model.WithImpliedBy(trigger),
			)
			expanded = append(expanded, a)
		}
	}
	return expanded, nil
}

// expandTemplateSlice applies template expansion to each element in a string
// slice, returning a new slice with all templates resolved.
func expandTemplateSlice(scope render.Scope, node model.Node, values []string, data sourceTemplateData, context string) ([]string, error) {
	if len(values) == 0 {
		return values, nil
	}
	result := make([]string, len(values))
	for i, v := range values {
		expanded, err := expandTemplateStr(scope, node, v, data, fmt.Sprintf("%s/args[%d]", context, i))
		if err != nil {
			return nil, err
		}
		result[i] = expanded
	}
	return result, nil
}

// expandTemplateMap applies template expansion to each value in a string map,
// returning a new map with all templates resolved. Keys are not expanded.
func expandTemplateMap(
	scope render.Scope,
	node model.Node,
	values map[string]string,
	data sourceTemplateData, context string,
) (map[string]string, error) {
	if len(values) == 0 {
		return values, nil
	}
	result := make(map[string]string, len(values))
	for k, v := range values {
		expanded, err := expandTemplateStr(scope, node, v, data, fmt.Sprintf("%s/namedArgs[%s]", context, k))
		if err != nil {
			return nil, err
		}
		result[k] = expanded
	}
	return result, nil
}

// packageTemplateData converts a *model.Package into the small struct exposed under
// .Package in templates. A nil package yields an empty struct.
func packageTemplateData(pkg *model.Package) sourcePackageData {
	if pkg == nil {
		return sourcePackageData{}
	}
	return sourcePackageData{
		Name: pkg.Name,
		Path: pkg.Path,
	}
}

// expandTemplateStr parses and executes s as a text/template against data.
// If s contains no template delimiters it is returned as-is.
func expandTemplateStr(scope render.Scope, node model.Node, s string, data sourceTemplateData, name string) (string, error) {
	if !strings.Contains(s, "{{") {
		return s, nil
	}

	pos := node.GetPosition()

	payload := map[any]any{
		"Name":    data.Name,
		"Output":  data.Output,
		"Package": data.Package,
		"Type":    data.Type,
		"Source":  data.Source,
	}
	maps.Copy(payload, scope)

	pos.Filename = path.Base(pos.Filename)

	tmpl, err := template.New(name).
		Option("missingkey=error").
		Funcs(render.GenericFunctions()).
		Funcs(map[string]any{
			"filename_suffixed": func(suffix string) string {
				output := toOutputTemplateData(path.Join(node.GetPackage().Dir, pos.Filename))
				return fmt.Sprintf("%s_%s%s", output.Name, suffix, output.Ext)
			},
		}).Parse(s)

	if err != nil {
		return "", fmt.Errorf("parsing template %q: %w", s, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, payload); err != nil {
		return "", fmt.Errorf("executing template %q: %w", s, err)
	}

	return buf.String(), nil
}
