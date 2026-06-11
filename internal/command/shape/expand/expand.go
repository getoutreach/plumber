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

	"github.com/Masterminds/sprig/v3"
	"github.com/getoutreach/plumber/internal/astx/inspect"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/validate"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
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

func MacroConfig(macro *config.PlumberMacroConfig) {
	annotations := inspect.ParseAnnotationsCommented(macro.Content)
	annotations = lo.Map(annotations, func(a model.Annotation, _ int) model.Annotation {
		// We want to preserve the original Args and NamedArgs of the macro annotation on the
		// implied annotations so that they can be used as template data in the deferred expansion stage.
		return model.NewAnnotation(
			a.Name, a.Args,
			model.WithNamedArgs(a.NamedArgs),
		)
	})

	for _, a := range annotations {
		macro.Annotations = append(macro.Annotations, config.AnnotationConfig{
			Name:      a.Name,
			Args:      a.Args,
			NamedArgs: a.NamedArgs,
		})
	}
}

// Macros replaces macro annotations with their defined annotation lists on all nodes
// across all packages. This runs before Walk and buildTransformers so that macros can inject
// entry-point annotations like plumber:derive or plumber:shape.
// Macro invocation arguments are validated against the macro's schema (if defined)
// before expansion.
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

	// Pre-compile JSON Schemas for macros that define them so that invocation
	// arguments can be validated before expansion.
	macroSchemas, err := compileMacroSchemas(macroMap)
	if err != nil {
		return fmt.Errorf("compiling macro schemas: %w", err)
	}

	for _, pkg := range pkgs {
		for _, typ := range pkg.Types {
			anns, err := Annotations(pkg, typ.TypeNode.Annotations, macroMap, macroSchemas)
			if err != nil {
				return err
			}
			typ.TypeNode.Annotations = anns
		}
		for _, fun := range pkg.Functions {
			anns, err := Annotations(pkg, fun.TypeNode.Annotations, macroMap, macroSchemas)
			if err != nil {
				return err
			}
			fun.TypeNode.Annotations = anns
		}
		for _, v := range pkg.Vars {
			anns, err := Annotations(pkg, v.TypeNode.Annotations, macroMap, macroSchemas)
			if err != nil {
				return err
			}
			v.TypeNode.Annotations = anns
		}
		for _, comment := range pkg.Comments {
			anns, err := Annotations(pkg, comment.Annotations, macroMap, macroSchemas)
			if err != nil {
				return err
			}
			comment.Annotations = anns
		}
	}
	return nil
}

// compileMacroSchemas compiles JSON Schema definitions from macro configs into
// a map keyed by macro name. Macros without a schema are omitted.
func compileMacroSchemas(macroMap map[string]*config.PlumberMacroConfig) (map[string]*validate.CompiledSchema, error) {
	schemas := make(map[string]*validate.CompiledSchema, len(macroMap))
	for name, macro := range macroMap {
		if macro.Schema == nil {
			continue
		}
		cs, err := validate.CompileSchema(name, macro.Schema)
		if err != nil {
			return nil, err
		}
		if cs != nil {
			schemas[name] = cs
		}
	}
	return schemas, nil
}

// expandAnnotations replaces any annotation whose name matches a macro with the macro's
// defined annotation list, preserving the order of non-macro annotations. The
// macro's Args and NamedArgs values are intentionally NOT template-expanded here
// — they are appended verbatim and each child annotation records the triggering
// macro annotation via ImpliedBy. Template expansion is deferred to a later stage
// (see TransformerAnnotations) so that both macro- and mixin-implied annotations
// can be expanded uniformly using their ImpliedBy reference as the data source.
//
// Before expansion, the macro invocation's positional and named arguments are
// validated against the macro's compiled JSON Schema (if one was provided in the
// macro configuration). Validation failures cause an immediate error.
//
// The pkg argument is preserved for symmetry and future use but is no longer
// consulted here since no template execution occurs at this stage.
func Annotations(
	pkg *model.Package, annotations model.Annotations, macroMap map[string]*config.PlumberMacroConfig,
	schemas map[string]*validate.CompiledSchema,
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

		// Validate macro invocation arguments against the macro's JSON Schema
		// before expanding. This catches invalid arguments early.
		if cs, hasSchema := schemas[ann.Name]; hasSchema {
			if err := validate.Annotation(ann, cs); err != nil {
				return nil, fmt.Errorf("macro %q: %w", ann.Name, err)
			}
		}

		// Capture a stable pointer to the triggering annotation that subsequent
		// expansion outputs can reference via ImpliedBy. We allocate a copy so the
		// referent's lifetime is independent of the input slice's storage.
		trigger := ann

		var annotations model.Annotations
		if macro.Content != "" {
			annotations = inspect.ParseAnnotationsCommented(macro.Content)
			annotations = lo.Map(annotations, func(a model.Annotation, _ int) model.Annotation {
				// We want to preserve the original Args and NamedArgs of the macro annotation on the
				// implied annotations so that they can be used as template data in the deferred expansion stage.
				return model.NewAnnotation(
					a.Name, a.Args,
					model.WithNamedArgs(a.NamedArgs),
					model.WithImpliedBy(trigger),
					model.WithSource(ann.Source),
					model.WithDocLine(ann.DocLine),
				)
			})
		}

		for _, macroAnn := range macro.Annotations {
			a := model.NewAnnotation(
				macroAnn.Name, macroAnn.Args,
				model.WithNamedArgs(macroAnn.NamedArgs),
				model.WithImpliedBy(trigger),
				model.WithSource(ann.Source),
				model.WithDocLine(ann.DocLine),
			)
			expanded = append(expanded, a)
		}
		expanded = append(expanded, annotations...)
	}
	return expanded, nil
}

// expandTemplateSlice applies template expansion to each element in a string
// slice, returning a new slice with all templates resolved.
func expandTemplateSlice(
	structurePathResolver contract.StructurePathResolver,
	scope render.Scope, node model.Node,
	values []string, data sourceTemplateData, context string,
) ([]string, error) {
	if len(values) == 0 {
		return values, nil
	}
	result := make([]string, len(values))
	for i, v := range values {
		expanded, err := expandTemplateStr(structurePathResolver, scope, node, v, data, fmt.Sprintf("%s/args[%d]", context, i))
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
	structurePathResolver contract.StructurePathResolver,
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
		expanded, err := expandTemplateStr(structurePathResolver, scope, node, v, data, fmt.Sprintf("%s/namedArgs[%s]", context, k))
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
func expandTemplateStr(
	structurePathResolver contract.StructurePathResolver,
	scope render.Scope, node model.Node,
	s string, data sourceTemplateData, name string,
) (string, error) {
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

	fm, dispose := WithRenderFuncMap(node, structurePathResolver, data)
	defer dispose()

	tmpl, err := template.New(name).
		Option("missingkey=error").
		Funcs(render.GenericFunctions()).
		Funcs(sprig.TxtFuncMap()).
		Funcs(gen.BaseFuncMap).
		Funcs(fm).
		Parse(s)

	if err != nil {
		return "", fmt.Errorf("parsing template %q: %w", s, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, payload); err != nil {
		return "", fmt.Errorf("executing template %q: %w", s, err)
	}

	return buf.String(), nil
}
