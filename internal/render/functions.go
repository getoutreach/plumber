// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements template functions registered into the gen engine for type
// formatting, import path resolution, and annotation lookups during rendering.

package render

import (
	"fmt"
	"maps"
	"path"
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

func TypesRenderer(
	currentPkgPath string,
	register *ModuleRegister,
	pathResolver PathResolverFunc) func(spec model.TypeSpec) (string, error) {
	return func(spec model.TypeSpec) (string, error) {
		fqn, err := astx.ParseFQN(spec.FQN)
		if err != nil {
			return "", fmt.Errorf("failed to parse FQN: %w", err)
		}

		// Resolve any structure:-prefixed package paths before localizing.
		if pathResolver != nil {
			var resolveErr error
			fqn.TranslateModules(func(pkgPath, typeName string) (string, bool) {
				resolved, err := pathResolver(pkgPath)
				if err != nil {
					resolveErr = err
					return pkgPath, false
				}
				if resolved != pkgPath {
					return resolved, true
				}
				return pkgPath, false
			})
			if resolveErr != nil {
				return "", fmt.Errorf("failed to resolve path in FQN: %w", resolveErr)
			}
		}

		fqn.Localize(func(pkgPath, typeName string) (string, bool) {
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

func placeholder(scope Scope) func(name ...string) string {
	return func(name ...string) string {
		if scope["Mode"] == ModeInPlace {
			return ""
		}
		return fmt.Sprintf("// <<plumber::Block(%s)>>\n// <</plumber::Block>>\n", strings.Join(name, "-"))
	}
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

func moduleImport(context Context) func(modulePath ...string) (string, error) {
	return func(modulePath ...string) (string, error) {
		m, err := module(context)(modulePath...)
		if err != nil {
			return "", err
		}
		m.use()
		return "", nil
	}
}

func module(context Context) func(modulePath ...string) (ModuleRef, error) {
	modules := context.GetModules()
	return func(modulePath ...string) (ModuleRef, error) {
		var err error
		if len(modulePath) == 0 {
			return ModuleRef{}, fmt.Errorf("module path is required")
		}
		p := modulePath[0]
		alias := ""
		if len(modulePath) > 1 {
			alias = modulePath[1]
		}

		if strings.HasPrefix(p, "../") || strings.HasPrefix(p, "./") {
			p = path.Clean(path.Join(context.GetPkgPath(), p))
		}

		resolve := context.GetPathResolver()

		if resolve != nil {
			p, err = resolve(p)
			if err != nil {
				return ModuleRef{}, fmt.Errorf("failed to resolve path: %w", err)
			}
		}

		if alias == "." {
			modules.Dot(p)
			return ModuleRef{}, nil
		}

		return ModuleRef{
			Path: p,
			use: func() ModuleRegistration {
				return modules.Register(p, astx.IsStandardType(p))
			}}, nil
	}
}

// ModuleRef represents a reference to a module that has been registered for inclusion
// during rendering, containing the module path and its registration details.
type ModuleRef struct {
	Path string
	use  func() ModuleRegistration
}

func (r ModuleRef) Type(name string) (model.TypeSpec, error) {
	r.use() // Ensure the module is registered for import
	fqn, err := astx.CraftFQN(r.Path, name)
	if err != nil {
		return model.TypeSpec{}, fmt.Errorf("failed to craft FQN for type %q in module %q: %w", name, r.Path, err)
	}
	return model.TypeSpec{FQN: fqn.String()}, nil
}

func (r ModuleRef) Ident(name string) string {
	reg := r.use() // Ensure the module is registered for import
	if reg.ID != "" && reg.ID != "." {
		return fmt.Sprintf("%s.%s", reg.ID, name)
	}
	return name
}

func (r ModuleRef) FQN(name string) (string, error) {
	// We don't want to call "use" here since we don't want to register the module for import
	fqn, err := astx.CraftFQN(r.Path, name)
	if err != nil {
		return "", fmt.Errorf("failed to craft FQN for type %q in module %q: %w", name, r.Path, err)
	}
	return fqn.String(), nil
}

func fileDescription(scope Scope) func(s string) string {
	return func(s string) string {
		if f, ok := scope["File"].(Scope); ok {
			f["Description"] = s
		}
		return ""
	}
}

func filePackageDescription(scope Scope) func(s string) string {
	return func(s string) string {
		if f, ok := scope["File"].(Scope); ok {
			f["PackageDescription"] = s
		}
		return ""
	}
}

func typeSet(context Context, set func(*model.Type)) func(string) (string, error) {
	return func(name string) (string, error) {
		fqn, err := astx.CraftFQN(context.GetPkgPath(), name)
		if err != nil {
			return "", fmt.Errorf("failed to craft FQN for type %q: %w", name, err)
		}
		tp := &model.Type{
			Spec: model.TypeSpec{
				FQN: fqn.String(),
			},
			TypeNode: &model.TypeNode{
				Position: model.Position{
					Filename: context.GetOutput(),
				},
			},
		}
		set(tp)
		return "", nil
	}
}

func typeMethodUndefined(context Context, tpFunc func() *model.Type) func(methodName string) (bool, error) {
	return func(methodName string) (bool, error) {
		tp := tpFunc()
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
	}
}

func WithGenericFuncMap(context Context) (opt gen.RenderOptionsFunc) {
	return gen.WithFuncMap(GenericFunctions())
}

func commentWrap(s string) string {
	parts := strings.Split(strings.TrimSpace(s), "\n")
	for i, part := range parts {
		parts[i] = "// " + part
	}
	return strings.Join(parts, "\n")
}
