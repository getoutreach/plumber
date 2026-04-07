package render

import (
	"fmt"
	"path"
	"reflect"
	"strconv"
	"strings"

	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/render/view"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

type typeScope struct {
	Type  model.Type
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
		return &typeScope{Type: model.Type{}, Scope: scope}, nil
	case *typeScope:
		for k := range v.Scope {
			scope[k] = v.Scope[k]
		}
		return &typeScope{Type: v.Type, Scope: scope}, nil
	default:
		return nil, fmt.Errorf("invalid type for extend: %T", v)
	}
}

type ModuleRegistration struct {
	Name    string
	ID      string
	Counter int
}

func (r ModuleRegistration) IsStd() bool {
	return r.ID == r.Name
}

func (r ModuleRegistration) IsPrimary() bool {
	return r.ID == path.Base(r.Name)
}

func (r *ModuleRegistration) Inc() int {
	r.Counter++
	return r.Counter
}

func (r ModuleRegistration) String() string {
	if r.IsStd() || r.IsPrimary() {
		return fmt.Sprintf("%q", r.Name)
	}
	return fmt.Sprintf("%s %q", r.ID, r.Name)
}

type ModuleRegister struct {
	presence map[string]ModuleRegistration
	Imports  []ModuleRegistration
}

func NewModuleRegister() *ModuleRegister {
	return &ModuleRegister{
		presence: make(map[string]ModuleRegistration),
	}
}

func (r *ModuleRegister) Registrations() map[string]ModuleRegistration {
	return r.presence
}

func (r *ModuleRegister) Dot(name string) ModuleRegistration {
	r.presence[name] = ModuleRegistration{Name: name, ID: "."}
	r.Imports = append(r.Imports, r.presence[name])
	return r.presence[name]
}

func (r *ModuleRegister) Register(name string, std bool) ModuleRegistration {
	if _, exists := r.presence[name]; !exists {
		id := name
		if !std {
			packageName := path.Base(name)
			reg, alreadyExists := lo.Find(r.Imports, func(reg ModuleRegistration) bool {
				return packageName == reg.ID
			})
			if alreadyExists {
				id = packageName + "_" + strconv.Itoa(reg.Inc())
			} else {
				id = packageName
			}
		}
		reg := ModuleRegistration{Name: name, ID: id}
		r.presence[name] = reg
		r.Imports = append(r.Imports, reg)
		return reg
	}
	return r.presence[name]
}

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

func typesRendererWithWrapper(currentPkgPath string, register *ModuleRegister, wrapper TypeWrapperProvider) func(o any, spec model.TypeSpec) (string, error) {
	c := typesRenderer(currentPkgPath, register)
	return func(o any, spec model.TypeSpec) (string, error) {
		if n, ok := o.(model.AnnotationProvider); ok {
			wn := n.GetAnnotations().Find("plumber:field_wrapper")
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
		return "", fmt.Errorf("%T does not implement model.AnnotationProvider\n", o)
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
		if a, ok := lo.Find(n.GetAnnotations(), func(a model.Annotation) bool { return a.Name == "plumber:comment" }); ok {
			return "// " + a.Value() + "\n"
		}
	} else {
		fmt.Printf("%T does not implement model.AnnotationProvider\n", o)
	}
	return ""
}

func placeholder(name ...string) string {
	return fmt.Sprintf("// <<plumber::Block(%s)>>\n// <</plumber::Block>>\n", strings.Join(name, "-"))
}

func ignored(ignores *Ignores) func(groups ...string) bool {
	return func(groups ...string) bool {
		return ignores.Ignored(groups...)
	}
}

func filterElements(provider any, elements any, groups ...string) (any, error) {
	array := reflect.ValueOf(elements)
	annotations, ok := provider.(model.AnnotationProvider)

	if !ok {
		return nil, fmt.Errorf("filterElements:%T does not implement model.AnnotationProvider", provider)
	}

	filters := annotations.GetAnnotations().FindAll("plumber:filter")

	// find filters that match the groups or have no subject
	filters = lo.Filter(filters, func(f model.Annotation, _ int) bool {
		subject, ok := f.NamedArgs["subject"]
		return !ok || lo.Contains(groups, subject)
	})

	if array.Kind() != reflect.Slice && array.Kind() != reflect.Array {
		return nil, fmt.Errorf("expected slice or array, got %s", array.Kind())
	}

	len := array.Len()

	filtered := reflect.MakeSlice(array.Type(), 0, len)

	for i := 0; i < len; i++ {
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
		} else {
			return false, fmt.Errorf("filterElement: %T does not implement model.AnnotationProvider", element)
		}
	}
	return false, nil
}
