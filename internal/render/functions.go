// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements template functions registered into the gen engine for type
// formatting, import path resolution, and annotation lookups during rendering.

package render

import (
	"fmt"
	"html/template"
	"maps"
	"strings"

	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/command/shape/render/view"
	"github.com/getoutreach/plumber/internal/genius/gen"

	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// typeScope is a helper struct used in template functions to maintain the current type being
// rendered along with a scope for additional variables.
type typeScope struct {
	Type  *model.Type
	Scope Scope
}

func extend(v any, kv ...any) (any, error) {
	if len(kv)%2 != 0 {
		return nil, fmt.Errorf("invalid number of parameters: %d", len(kv))
	}
	scope := make(Scope, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		scope[kv[i]] = kv[i+1]
	}
	switch v := v.(type) {
	case *view.Struct:
		n := make(Scope)
		maps.Copy(n, v.Scope)
		maps.Copy(n, scope)
		return &typeScope{Type: v.Type, Scope: n}, nil
	case *typeScope:
		n := make(Scope)
		maps.Copy(n, v.Scope)
		maps.Copy(n, scope)
		return &typeScope{Type: v.Type, Scope: n}, nil
	default:
		return nil, fmt.Errorf("invalid type for extend: %T", v)
	}
}

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

func TypesRenderer(currentPkgPath string, register *ModuleRegister) func(spec model.TypeSpec) (string, error) {
	return func(spec model.TypeSpec) (string, error) {
		fqn, err := astx.ParseFQN(spec.FQN)
		if err != nil {
			return "", fmt.Errorf("failed to parse FQN: %w", err)
		}
		fqn.WalkPackages(func(pkgPath, typeName string) (string, bool) {
			if pkgPath == currentPkgPath {
				return "", true
			}
			id := register.Register(pkgPath, astx.IsStandardType(pkgPath)).ID
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

func annotation(o any, name string) *model.Annotation {
	if n, ok := o.(model.AnnotationProvider); ok {
		for _, ann := range n.GetAnnotations() {
			if ann.Name == name {
				return &ann
			}
		}
	}
	return nil
}

func AnnotationValue(o any, name string) string {
	if ann := annotation(o, name); ann != nil && len(ann.Args) > 0 {
		return ann.Args[0]
	}
	return ""
}

func placeholder(name ...string) string {
	return fmt.Sprintf("// <<plumber::Block(%s)>>\n// <</plumber::Block>>\n", strings.Join(name, "-"))
}

func fragmentStart(scope Scope) func(name ...string) string {
	return func(name ...string) string {
		if scope["Mode"] == ModeInPlace {
			return ""
		}
		return fmt.Sprintf("// <<plumber::Fragment(%s)>>\n", strings.Join(name, "-"))
	}
}

func fragmentEnd(scope Scope) func() string {
	return func() string {
		if scope["Mode"] == ModeInPlace {
			return ""
		}
		return "// <</plumber::Fragment>>"
	}
}

func ignored(ignores *Ignores) func(groups ...string) bool {
	return func(groups ...string) bool {
		return ignores.Ignored(groups...)
	}
}

func moduleInclude(context Context) func(modulePath string) (string, error) {
	modules := context.GetModules()
	return func(modulePath string) (string, error) {
		modules.Register(modulePath, !strings.Contains(modulePath, "/"))
		return "", nil
	}
}

func WithRenderFuncMap(context Context, scope Scope, output string) (opt gen.RenderOptionsFunc, dispose func()) {
	var tp *model.Type
	dispose = func() {
		if tp != nil {
			tp = nil
		}
	}
	functions := template.FuncMap{
		"extend": extend,

		"type": TypesRenderer(context.GetPkgPath(), context.GetModules()),
		"type_set": func(name string) (string, error) {
			fqn, err := astx.CraftFQN(context.GetPkgPath(), name)
			if err != nil {
				return "", fmt.Errorf("failed to craft FQN for type %q: %w", name, err)
			}
			tp = &model.Type{
				Spec: model.TypeSpec{
					FQN: fqn.String(),
				},
				TypeNode: &model.TypeNode{
					Position: model.Position{
						Filename: context.GetOutput(),
					},
				},
			}
			return "", nil
		},
		"type_method_undefined": func(methodName string) (bool, error) {
			if tp == nil {
				return false, fmt.Errorf("type not set by type_set function")
			}
			_, ok := lo.Find(context.GetPackage().Types, func(t *model.Type) bool {
				if t.Spec.FQN == tp.Spec.FQN {
					for _, m := range t.Struct.Methods {
						if m.Name == methodName {
							if m.Position.Filename != context.GetOutput() {
								return true
							}
						}
					}
				}
				return false
			})
			return !ok, nil
		},
		"placeholder":    placeholder,
		"fragment_start": fragmentStart(scope),
		"fragment_end":   fragmentEnd(scope),
		"module_include": moduleInclude(context),
	}
	maps.Copy(functions, GenericFunctions())
	return gen.WithFuncMap(functions), dispose
}

func GenericFunctions() template.FuncMap {
	return template.FuncMap{
		"annotation":       annotation,
		"annotation_value": AnnotationValue,
		"fqn_mask": func(spec model.TypeSpec, mask string) (string, error) {
			fqn, err := astx.ParseFQN(spec.FQN)
			if err != nil {
				return "", fmt.Errorf("failed to parse FQN: %w", err)
			}
			return fqn.Mask(mask).String(), nil
		},
	}
}

func WithGenericFuncMap(context Context) (opt gen.RenderOptionsFunc) {
	return gen.WithFuncMap(GenericFunctions())
}
