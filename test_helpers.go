package plumber

import (
	"fmt"
	"reflect"
)

// SavePoint snapshots the state of the supplied dependencies AND of every
// dependency in the container that (transitively) declares a dependency on
// any of them. This ensures that when the returned restore function is
// invoked, dependents that were re-resolved against a redefined upstream
// dep are also rolled back to their prior state.
//
// This mechanism is intended to be used in tests that need to temporarily redefine a
// dependency and want to ensure that all affected dependents are properly rolled back to their prior state.
//
// Intuitive approach of cloning whole container is not viable because of closures
// used during definition keeps pointers to the original container and its dependencies.
//
// Dependencies that do not implement the savepointable interface cause an
// error to be returned.
func SavePoint(a ContainerLike, deps ...Dependency) (restore func(), err error) {
	// Validate and snapshot all explicitly supplied deps first.
	toSave := make([]Dependency, 0, len(deps))
	seen := map[Dependency]struct{}{}
	for _, d := range deps {
		if _, ok := d.(savepointable); !ok {
			return nil, fmt.Errorf("%T is not savepointable", d)
		}
		if _, dup := seen[d]; dup {
			continue
		}
		seen[d] = struct{}{}
		toSave = append(toSave, d)
	}

	// Walk the container to discover all transitive dependents of the supplied
	// deps so their state is captured too.
	allDeps := collectAllDependencies(a)
	reverseDeps := map[Dependency][]Dependency{}
	for _, d := range allDeps {
		if r, ok := d.(resettable); ok {
			for _, sub := range r.directDeps() {
				reverseDeps[sub] = append(reverseDeps[sub], d)
			}
		}
	}

	queue := append([]Dependency(nil), toSave...)
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, dependent := range reverseDeps[current] {
			if _, ok := seen[dependent]; ok {
				continue
			}
			seen[dependent] = struct{}{}
			// Only include dependents that can be snapshotted; ignore the
			// rest so a non-savepointable dependent doesn't break an
			// otherwise valid SavePoint of the user's explicit deps.
			if _, ok := dependent.(savepointable); ok {
				toSave = append(toSave, dependent)
			}
			queue = append(queue, dependent)
		}
	}

	restoreFuncs := make([]func(), 0, len(toSave))
	for _, d := range toSave {
		restoreFuncs = append(restoreFuncs, d.(savepointable).savePoint())
	}

	return func() {
		// Restore in reverse order so dependents are rolled back before their
		// upstream deps, mirroring how they were captured.
		for i := len(restoreFuncs) - 1; i >= 0; i-- {
			if restoreFuncs[i] != nil {
				restoreFuncs[i]()
			}
		}
	}, nil
}

// SavePointResetDefinition is a helper that combines SavePoint and ResetDefinition for the common test pattern of "save state,
// reset definition, restore state".
func SavePointResetDefinition(a ContainerLike, deps ...Dependency) (restore func(), err error) {
	restore, err = SavePoint(a, deps...)
	if err == nil {
		err = ResetDefinition(a, deps...)
	}
	return restore, err
}

// ResetDefinition performs a full reset on the given dependencies, clearing their definitions
// (including sync.Once) so they can be re-defined via Define/Const/Resolver. It also cascades
// the invalidation to all transitive dependents in the container, clearing their resolved state.
func ResetDefinition(a ContainerLike, deps ...Dependency) error {
	for _, d := range deps {
		if r, ok := d.(resettable); ok {
			r.resetDefined()
		} else {
			return fmt.Errorf("%T is not resettable", d)
		}
	}
	return Reset(a, deps...)
}

// Reset allows to reset resolved value for dependencies. It can be used in tests to reset
// resolved value and force re-evaluation of the dependency with new definition or wrappers.
//
// Reset(&c.SomeDependency) will reset resolved value for SomeDependency and all dependencies that depend on it.
//
// All affected dependencies have their resolved state cleared so the next Instance() call
// re-triggers resolution. Use ResetDefinition to reset a dependency's definition when needed.
func Reset(a ContainerLike, deps ...Dependency) error {
	// Collect all dependencies from the container via reflection.
	allDeps := collectAllDependencies(a)

	// Build reverse dependency map: for each dep X, for each direct dependency Y of X,
	// record Y -> [X]. This lets us find "who depends on Y" (i.e. who needs to be
	// invalidated when Y is reset).
	reverseDeps := map[Dependency][]Dependency{}
	for _, d := range allDeps {
		if r, ok := d.(resettable); ok {
			for _, sub := range r.directDeps() {
				reverseDeps[sub] = append(reverseDeps[sub], d)
			}
		}
	}

	// BFS from the explicitly given deps through the reverse map to collect the full
	// transitive set of dependents that need resetting.
	seen := map[Dependency]struct{}{}
	queue := make([]Dependency, 0, len(deps))
	for _, d := range deps {
		seen[d] = struct{}{}
		queue = append(queue, d)
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, dependent := range reverseDeps[current] {
			if _, ok := seen[dependent]; !ok {
				seen[dependent] = struct{}{}
				queue = append(queue, dependent)
			}
		}
	}

	// Clear the resolved state on all collected dependencies so they
	// re-evaluate on the next Instance() call.
	for dep := range seen {
		if r, ok := dep.(resettable); ok {
			r.resetResolved()
		}
	}

	return nil
}

// WrapperLike is an interface for dependencies that can be used as wrappers and can be disabled via DisableWrapper in tests.
type WrapperLike[T any] interface {
	Dependency
	Define(func() func(T) T)
}

// DisableWrapper is a helper function to define a wrapper that does not modify the original value.
// It can be used for testing purposes to disable particular wrapper
func DisableWrapper[T any](a ContainerLike, wrapperDefiner WrapperLike[T]) error {
	if err := ResetDefinition(a, wrapperDefiner); err != nil {
		return err
	}
	wrapperDefiner.Define(func() func(T) T {
		return func(a T) T {
			return a
		}
	})
	return nil
}

// collectAllDependencies uses reflection to walk the container struct tree
// and collect all fields that implement the Dependency interface.
func collectAllDependencies(container interface{}) []Dependency {
	var result []Dependency
	collectDepsRecursive(reflect.ValueOf(container), &result)
	return result
}

// collectDepsRecursive recursively walks struct fields, collecting Dependency instances.
func collectDepsRecursive(v reflect.Value, result *[]Dependency) {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if !field.CanInterface() {
			continue
		}

		// Try addressable value-type fields (e.g. D[T] stored as value in struct).
		// Taking the address gives us *D[T] which implements Dependency.
		if field.CanAddr() {
			if dep, ok := field.Addr().Interface().(Dependency); ok {
				*result = append(*result, dep)
				continue
			}
		}

		// Try pointer-type fields directly.
		if dep, ok := field.Interface().(Dependency); ok {
			*result = append(*result, dep)
			continue
		}

		// Recurse into struct fields (sub-containers).
		f := reflect.Indirect(field)
		if f.Kind() == reflect.Struct {
			collectDepsRecursive(field, result)
		}
	}
}
