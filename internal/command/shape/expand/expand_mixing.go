// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file define an expansion of mixins
package expand

import (
	"fmt"

	"github.com/getoutreach/plumber/internal/astx/inspect"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

func Mixin(annotation model.Annotation, lastTransformer contract.Transformer, mixins []config.MixinConfig) error {
	mixinName := annotation.Value()
	mixinConfig, ok := lo.Find(mixins, func(mixin config.MixinConfig) bool {
		return mixin.PlumberMixin != nil && mixin.PlumberMixin.Name == mixinName
	})
	if !ok {
		return fmt.Errorf("mixin %q not found in config", mixinName)
	}
	// Stable copy of the triggering plumber:mixin annotation so each
	// expanded child annotation can record its provenance via ImpliedBy.
	trigger := annotation
	for _, mixinAnnotation := range mixinConfig.PlumberMixin.Annotations {
		if !lastTransformer.Accepts(mixinAnnotation.Name) {
			return fmt.Errorf(
				"transformer %s does not accept annotation %q from mixin %q",
				lastTransformer.GetName(), mixinAnnotation.Name, mixinName,
			)
		}
		a := model.NewAnnotation(
			mixinAnnotation.Name, mixinAnnotation.Args,
			model.WithNamedArgs(mixinAnnotation.NamedArgs),
			model.WithImpliedBy(trigger),
		)
		lastTransformer.Add(a)
	}
	// Parse optional Content (raw commented-annotation text) and emit each
	// parsed annotation as an additional implied annotation, mirroring the
	// behavior of PlumberMacroConfig.Content handling in expand.Annotations.
	if mixinConfig.PlumberMixin.Content != "" {
		contentAnnotations := inspect.ParseAnnotationsCommented(mixinConfig.PlumberMixin.Content)
		for _, ca := range contentAnnotations {
			if !lastTransformer.Accepts(ca.Name) {
				return fmt.Errorf(
					"transformer %s does not accept annotation %q from mixin %q",
					lastTransformer.GetName(), ca.Name, mixinName,
				)
			}
			a := model.NewAnnotation(
				ca.Name, ca.Args,
				model.WithNamedArgs(ca.NamedArgs),
				model.WithImpliedBy(trigger),
			)
			lastTransformer.Add(a)
		}
	}
	return nil
}
