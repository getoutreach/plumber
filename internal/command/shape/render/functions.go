// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements template functions registered into the gen engine for type
// formatting, import path resolution, and annotation lookups during rendering.

package render

import (
	"fmt"
	"html/template"
	"reflect"
	"strings"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/expand"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
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

func typesRendererWithWrapper(
	currentPkgPath string,
	register *render.ModuleRegister,
	wrapper TypeWrapperProvider,
) func(o any, spec model.TypeSpec, subjects ...any) (string, error) {
	c := render.TypesRenderer(currentPkgPath, register)
	return func(o any, spec model.TypeSpec, subjects ...any) (string, error) {
		if n, ok := o.(model.AnnotationProvider); ok {
			wn := n.GetAnnotations().Find(contract.OptionFieldWrapper)
			if wn != nil {
				wrapperType := wn.Value()

				// Use the first variadic subject for annotation-based matching,
				// falling back to the primary object when none is provided.
				var subject model.AnnotationProvider
				if len(subjects) > 0 {
					subject, _ = subjects[0].(model.AnnotationProvider)
				}
				if subject == nil {
					subject = n
				}

				wrapped, err := wrapper.WrapType(wrapperType, &spec, subject)
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
	name := render.AnnotationValue(o, contract.OptionName)
	name = strings.ToLower(name)
	if name == "" {
		return "r"
	}
	return name[:1]
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

func withRenderFuncMap(context *Context, output string) (opt gen.RenderOptionsFunc) {
	functions := template.FuncMap{
		"type_wrap":       typesRendererWithWrapper(context.GetPkgPath(), context.GetModules(), context.Wrapper),
		"ignored":         ignored(context.Ignores),
		"expand_name":     expand.Name,
		"comment":         comment,
		"filter_elements": filterElements,
		"receiver":        receiver,
	}
	return gen.WithFuncMap(functions)
}
