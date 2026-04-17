// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Feature interface and FeatureFunc adapter for composable code generation units.

package gen

import "strings"

// Feature represents a unit of code generation logic that can be rendered to produce output,
// allowing for modular and composable generation of code based on the context and writer provided.
type Feature interface {
	Render(*Context, *Writer) error
}

// FeatureFunc is an adapter that allows ordinary functions to be used as Features, enabling flexible
// composition of code generation logic.
type FeatureFunc func(*Context, *Writer) error

func Error(err error) FeatureFunc {
	return func(ctx *Context, w *Writer) error {
		return err
	}
}

func (f FeatureFunc) Render(ctx *Context, w *Writer) error {
	return f(ctx, w)
}

// Features represents a collection of Feature instances that can be rendered sequentially,
// allowing for composition of multiple features during code generation.
type Features []Feature

func (ff *Features) Error(err error) {
	if ff == nil {
		ff = &Features{}
	}
	*ff = append(*ff, Error(err))
}

func (ff Features) Render(ctx *Context, w *Writer) error {
	for _, f := range ff {
		ctx.Reset()
		if err := f.Render(ctx, w); err != nil {
			return err
		}
	}
	return nil
}

// NamedFeature represents a feature with an associated name, allowing for categorization and selective
// inclusion of features during code generation based on feature flags.
type NamedFeature struct {
	Name     string
	Features Features
}

// NamedFeatures represents a collection of NamedFeature instances that can be filtered based on feature flags,
// allowing for selective inclusion of features during code generation.
type NamedFeatures []NamedFeature

func (f NamedFeatures) Filter(flags *FeatureFlags) NamedFeatures {
	var res NamedFeatures
	for _, nf := range f {
		if flags.IsIncluded(nf.Name) {
			res = append(res, nf)
		}
	}
	return res
}

// FeatureFlags represents a set of feature flags that can be used to include or exclude specific features
// during code generation, allowing for flexible configuration of the generation process based on user-defined criteria.
type FeatureFlags struct {
	include map[string]struct{}
	exclude map[string]struct{}
}

func NewFeatureFlags() *FeatureFlags {
	return &FeatureFlags{
		include: make(map[string]struct{}),
		exclude: make(map[string]struct{}),
	}
}

func (i *FeatureFlags) String() string {
	return "feature flags"
}
func (i *FeatureFlags) Set(value string) error {
	vals := strings.Split(value, ",")
	for _, v := range vals {
		if strings.HasPrefix(v, "^") {
			v = strings.TrimPrefix(v, "^")
			i.exclude[v] = struct{}{}
		} else {
			i.include[v] = struct{}{}
		}
	}
	return nil
}

// IsIncluded returns true if the fature is included is set and not excluded
func (i *FeatureFlags) IsIncluded(value string) bool {
	if i.IsExcluded(value) {
		return false
	}
	_, ok := i.include[value]
	return ok || len(i.include) == 0
}

// IsExcluded returns true if the fature is excluded
func (i *FeatureFlags) IsExcluded(value string) bool {
	_, ok := i.exclude[value]
	return ok
}
