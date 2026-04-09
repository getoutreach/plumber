// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides Walk and annotation-filtering helpers for traversing the plumber query model node tree.

package inspect

import (
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

func Walk(pkgs []*model.Package, visitor func(node model.Node) error) error {
	for _, pkg := range pkgs {
		for _, typ := range pkg.Types {
			if err := visitor(typ); err != nil {
				return err
			}
		}
		for _, fun := range pkg.Functions {
			if err := visitor(fun); err != nil {
				return err
			}
		}
	}
	return nil
}

type AnnotationMatcher func(annotation model.Annotation) bool

func WithAnnotations(matcher AnnotationMatcher, visitor func(node model.Node) error) func(node model.Node) error {
	return func(node model.Node) error {
		for _, annotation := range node.GetNode().GetAnnotations() {
			if matcher(annotation) {
				return visitor(node)
			}
		}
		return nil
	}
}

func WithAnnotationName(names ...string) func(annotation model.Annotation) bool {
	return func(annotation model.Annotation) bool {
		return lo.Contains(names, annotation.Name)
	}
}
