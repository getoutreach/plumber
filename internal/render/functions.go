// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements template functions registered into the gen engine for type
// formatting, import path resolution, and annotation lookups during rendering.

package render

import (
	"fmt"
	"html/template"
	"reflect"
	"strings"

	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/render/view"
	"github.com/getoutreach/plumber/internal/genius/gen"

	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// typeScope is a helper struct used in template functions to maintain the current type being
// rendered along with a scope for additional variables.
type typeScope struct {
	Type  *model.Type
	Scope map[any]any
}

func extend(v any, kv ...any) (any, error) {
	if len(kv)%2 != 0 {
		return nil, fmt.Errorf("invalid number of parameters: %d", len(kv))
	}
	scope := make(map[any]any, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		scope[kv[i]] = kv[i+1]
	}
	switch v := v.(type) {
	case *view.Struct:
		for k := range v.Scope {
			scope[k] = v.Scope[k]
		}
		return &typeScope{Type: v.Type, Scope: scope}, nil
	case *typeScope:
		for k := range v.Scope {
			scope[k] = v.Scope[k]
		}
		return &typeScope{Type: v.Type, Scope: scope}, nil
	default:
		return nil, fmt.Errorf("invalid type for extend: %T", v)
	}
}

func expandName(v string, t *model.Type) any {
	return strings.ReplaceAll(v, "{name}", t.Name)
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

func typesRenderer(currentPkgPath string, register *ModuleRegister) func(spec model.TypeSpec) (string, error) {
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

func annotationValue(o any, name string) string {
	if ann := annotation(o, name); ann != nil && len(ann.Args) > 0 {
		return ann.Args[0]
	}
	return ""
}

func comment(o any) string {
	if n, ok := o.(model.AnnotationProvider); ok {
		if a, ok := lo.Find(n.GetAnnotations(), func(a model.Annotation) bool { return a.Name == contract.OptionComment }); ok {
			return "// " + a.Value() + "\n"
		}
	} else {
		fmt.Printf("%T does not implement model.AnnotationProvider\n", o)
	}
	return ""
}

func receiver(o any) string {
	if n, ok := o.(model.AnnotationProvider); ok {
		if a, ok := lo.Find(n.GetAnnotations(), func(a model.Annotation) bool { return a.Name == contract.OptionReceiver }); ok {
			return a.Value()
		}
	} else {
		fmt.Printf("%T does not implement model.AnnotationProvider\n", o)
	}
	name := annotationValue(o, contract.OptionName)
	name = strings.ToLower(name)
	if name == "" {
		return "r"
	}
	return name[:1]
}

func placeholder(name ...string) string {
	return fmt.Sprintf("// <<plumber::Block(%s)>>\n// <</plumber::Block>>\n", strings.Join(name, "-"))
}

func fragmentStart(scope map[string]any) func(name ...string) string {
	return func(name ...string) string {
		if scope["Mode"] == ModeInPlace {
			return ""
		}
		return fmt.Sprintf("// <<plumber::Fragment(%s)>>\n", strings.Join(name, "-"))
	}
}

func fragmentEnd(scope map[string]any) func() string {
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

func filterElements(provider, elements any, groups ...string) (any, error) {
	array := reflect.ValueOf(elements)
	annotations, ok := provider.(model.AnnotationProvider)

	if !ok {
		return nil, fmt.Errorf("filterElements:%T does not implement model.AnnotationProvider", provider)
	}

	filters := annotations.GetAnnotations().FindAll(contract.OptionFilter)

	// find filters that match the groups or have no subject
	filters = lo.Filter(filters, func(f model.Annotation, _ int) bool {
		subject, ok := f.NamedArgs["subject"]
		return !ok || lo.Contains(groups, subject)
	})

	if array.Kind() != reflect.Slice && array.Kind() != reflect.Array {
		return nil, fmt.Errorf("expected slice or array, got %s", array.Kind())
	}

	arrLen := array.Len()

	filtered := reflect.MakeSlice(array.Type(), 0, arrLen)

	for i := 0; i < arrLen; i++ {
		elem := array.Index(i).Interface()
		matches := true
		for _, f := range filters {
			ok, err := filterElement(elem, f)
			if err != nil {
				return nil, fmt.Errorf("failed to apply filter %q: %w", f.Name, err)
			}
			if !ok {
				matches = false
				break
			}
		}
		if matches {
			filtered = reflect.Append(filtered, array.Index(i))
		}
	}
	return filtered.Interface(), nil
}

func filterElement(element any, a model.Annotation) (bool, error) {
	val := a.Value()
	if val == "annotation.has" && len(a.Args) > 1 {
		annName := a.Args[1]
		if n, ok := element.(model.AnnotationProvider); ok {
			return n.GetAnnotations().Find(annName) != nil, nil
		}
		return false, fmt.Errorf("filterElement: %T does not implement model.AnnotationProvider", element)
	}
	return false, nil
}

func moduleInclude(context Context) func(modulePath string) (string, error) {
	modules := context.GetModules()
	return func(modulePath string) (string, error) {
		modules.Register(modulePath, strings.Contains(modulePath, "/"))
		return "", nil
	}
}

func WithRenderFuncMap(context Context, scope map[string]any, output string) (opt gen.RenderOptionsFunc, dispose func()) {
	var tp *model.Type
	dispose = func() {
		if tp != nil {
			tp = nil
		}
	}
	functions := template.FuncMap{
		"extend":      extend,
		"expand_name": expandName,
		"type":        typesRenderer(context.GetPkgPath(), context.GetModules()),
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
		"annotation":       annotation,
		"annotation_value": annotationValue,
		"comment":          comment,
		"filter_elements":  filterElements,
		"placeholder":      placeholder,
		"fragment_start":   fragmentStart(scope),
		"fragment_end":     fragmentEnd(scope),
		"receiver":         receiver,
		"module_include":   moduleInclude(context),
	}
	return gen.WithFuncMap(functions), dispose
}
