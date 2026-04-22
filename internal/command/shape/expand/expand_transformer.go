// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file define an expansion of transformers annotations
package expand

import (
	"fmt"

	"github.com/getoutreach/plumber/query/model"
)

// TransformerAnnotations performs the deferred per-annotation template expansion
// for annotations that were implied by a macro or mixin (i.e. their ImpliedBy
// field is non-nil). The triggering annotation referenced by ImpliedBy is
// exposed to the template under the .Source context (Args + NamedArgs); the
// supplied pkg is exposed under .Package (Name + Path). Annotations without an
// ImpliedBy reference are passed through unchanged.
//
// Expansion is performed annotation-by-annotation so it works uniformly for
// both macro-implied and mixin-implied annotations. A nil pkg degrades
// gracefully to empty .Package fields.
func TransformerAnnotations(pkg *model.Package, annotations model.Annotations) (model.Annotations, error) {
	if len(annotations) == 0 {
		return annotations, nil
	}
	result := make(model.Annotations, 0, len(annotations))
	for i := range annotations {
		ann := annotations[i]
		expanded, err := expandImpliedAnnotation(pkg, ann)
		if err != nil {
			return nil, err
		}
		result = append(result, expanded)
	}
	return result, nil
}

// expandImpliedAnnotation runs template expansion on the Args and NamedArgs of a
// single annotation when it was implied by another annotation (macro or mixin).
// Annotations without an ImpliedBy reference are returned unchanged.
func expandImpliedAnnotation(pkg *model.Package, ann model.Annotation) (model.Annotation, error) {
	if ann.ImpliedBy == nil {
		return ann, nil
	}

	data := sourceTemplateData{
		Source: sourceAnnotationData{
			Args:      ann.ImpliedBy.Args,
			NamedArgs: ann.ImpliedBy.NamedArgs,
		},
		Package: packageTemplateData(pkg),
	}

	args, err := expandTemplateSlice(ann.Args, data, ann.Name)
	if err != nil {
		return ann, fmt.Errorf("expanding implied annotation %q (from %q) args: %w", ann.Name, ann.ImpliedBy.Name, err)
	}

	namedArgs, err := expandTemplateMap(ann.NamedArgs, data, ann.Name)
	if err != nil {
		return ann, fmt.Errorf("expanding implied annotation %q (from %q) namedArgs: %w", ann.Name, ann.ImpliedBy.Name, err)
	}

	ann.Args = args
	ann.NamedArgs = namedArgs
	return ann, nil
}
