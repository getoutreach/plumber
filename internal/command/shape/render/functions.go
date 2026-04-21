// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements template functions registered into the gen engine for type
// formatting, import path resolution, and annotation lookups during rendering.

package render

import (
	"fmt"
	"html/template"
	"strings"

	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
)

// Ignores provides functionality to track and check for ignored groups during rendering,
// allowing for conditional rendering based on group membership.
type Ignores struct {
	presence map[string]struct{}
}

func NewIgnores(ignores ...[]string) *Ignores {
	i := &Ignores{
		presence: make(map[string]struct{}),
	}
	for _, group := range ignores {
		i.presence[strings.Join(group, "|||")] = struct{}{}
	}
	return i
}

func (i *Ignores) Ignored(groups ...string) bool {
	_, exists := i.presence[strings.Join(groups, "|||")]
	return exists
}

func typesRenderer(currentPkgPath string, register *render.ModuleRegister) func(spec model.TypeSpec) (string, error) {
	return func(spec model.TypeSpec) (string, error) {
		fqn, err := astx.ParseFQN(spec.FQN)
		if err != nil {
			return "", fmt.Errorf("failed to parse FQN: %w", err)
		}
		fqn.WalkPackages(func(pkgPath, typeName string) (string, bool) {
			if pkgPath == currentPkgPath {
				return "", true
			}
			id := register.Register(pkgPath, astx.StandardType(pkgPath)).ID
			if id == "." {
				return "", true
			}
			return id, true
		})
		if fqn.IsPackageLess() {
			return fqn.Unquote(), nil
		}
		return fqn.String(), nil
	}
}

func typesRendererWithWrapper(
	currentPkgPath string,
	register *render.ModuleRegister,
	wrapper TypeWrapperProvider,
) func(o any, spec model.TypeSpec) (string, error) {
	c := typesRenderer(currentPkgPath, register)
	return func(o any, spec model.TypeSpec) (string, error) {
		if n, ok := o.(model.AnnotationProvider); ok {
			wn := n.GetAnnotations().Find(contract.OptionFieldWrapper)
			if wn != nil {
				wrapperType := wn.Value()
				wrapped, err := wrapper.WrapType(wrapperType, &spec)
				if err != nil {
					return "", fmt.Errorf("failed to wrap type with wrapper %q: %w", wrapperType, err)
				}
				if wrapped != nil {
					return c(*wrapped)
				}
				return "", fmt.Errorf("wrapper %q returned nil", wrapperType)
			}
			return c(spec)
		}
		return "", fmt.Errorf("%T does not implement model.AnnotationProvider", o)
	}
}

func ignored(ignores *Ignores) func(groups ...string) bool {
	return func(groups ...string) bool {
		return ignores.Ignored(groups...)
	}
}

func withRenderFuncMap(context *Context, output string) (opt gen.RenderOptionsFunc) {
	functions := template.FuncMap{
		"type_wrap": typesRendererWithWrapper(context.GetPkgPath(), context.GetModules(), context.Wrapper),
		"ignored":   ignored(context.Ignores),
	}
	return gen.WithFuncMap(functions)
}
