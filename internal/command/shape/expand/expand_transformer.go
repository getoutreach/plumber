// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file define an expansion of transformers annotations
package expand

import (
	"fmt"
	"path"
	"strings"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
)

// TransformerAnnotations performs the deferred per-annotation template expansion
// for annotations that were implied by a macro or mixin (i.e. their ImpliedBy
// field is non-nil). The triggering annotation referenced by ImpliedBy is
// exposed to the template under the .Source context (Args + NamedArgs); the
// package derived from node is exposed under .Package (Name + Path); and the
// node itself is exposed under .Type so templates may inspect type metadata
// (e.g. `{{ .Type.GetAnnotations }}`). Annotations without an ImpliedBy
// reference are passed through unchanged.
//
// Expansion is performed annotation-by-annotation so it works uniformly for
// both macro-implied and mixin-implied annotations. A nil node degrades
// gracefully to empty .Package fields and a nil .Type.
func TransformerAnnotations(node model.Node, annotations model.Annotations, scope render.Scope) (model.Annotations, error) {
	if len(annotations) == 0 {
		return annotations, nil
	}
	var pkg *model.Package
	if node != nil {
		pkg = node.GetPackage()
	}
	result := make(model.Annotations, 0, len(annotations))
	for i := range annotations {
		ann := annotations[i]
		expanded, err := expandAnnotationValue(
			scope,
			node,
			pkg,
			ann,
			annotations.FindOr(contract.OptionName).Value(),
			annotations.FindOr(contract.OptionOutput, "generated.go").Value(),
		)
		if err != nil {
			return nil, err
		}
		result = append(result, expanded)
	}

	return result, nil
}

// expandAnnotationValue runs template expansion on the Args and NamedArgs of a
// single annotation when it was implied by another annotation (macro or mixin).
// Annotations without an ImpliedBy reference are returned unchanged.
func expandAnnotationValue(scope render.Scope, node model.Node, pkg *model.Package, ann model.Annotation, name, output string) (model.Annotation, error) {
	// return rendered
	var n any = node
	if tn, ok := node.(*model.Type); ok {
		n = tn
	}
	data := sourceTemplateData{
		Package: packageTemplateData(pkg),
		Type:    n,
		Name:    name,
		Output:  toOutputTemplateData(output),
	}

	if ann.ImpliedBy != nil {
		data.Source = &sourceAnnotationData{
			Args:      ann.ImpliedBy.Args,
			NamedArgs: ann.ImpliedBy.NamedArgs,
		}
	}

	args, err := expandTemplateSlice(scope, node, ann.Args, data, ann.Name)
	if err != nil {
		return ann, fmt.Errorf("expanding implied annotation %q (from %q) args: %w", ann.Name, ann.ImpliedBy.Name, err)
	}

	namedArgs, err := expandTemplateMap(scope, node, ann.NamedArgs, data, ann.Name)
	if err != nil {
		return ann, fmt.Errorf("expanding implied annotation %q (from %q) namedArgs: %w", ann.Name, ann.ImpliedBy.Name, err)
	}

	ann.Args = args
	ann.NamedArgs = namedArgs
	return ann, nil
}

func toOutputTemplateData(fileName string) outputTemplateData {
	dir := path.Dir(fileName)
	baseFilename := path.Base(fileName)
	ext := path.Ext(baseFilename)
	name := strings.TrimSuffix(baseFilename, ext)
	return outputTemplateData{
		Filename: baseFilename,
		Name:     name,
		Ext:      ext,
		Dir:      dir,
	}
}
