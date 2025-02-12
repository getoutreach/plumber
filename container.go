// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains root dependency container

package plumber

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Container represents a root dependency container
type Container struct {
	cleanup []func(context.Context) error
}

// Cleanup registers a cleanup function
func (c *Container) Cleanup(fn func(context.Context)) {
	c.cleanup = append(c.cleanup, func(ctx context.Context) error {
		fn(ctx)
		return nil
	})
}

// CleanupError registers a cleanup function returning an error
func (c *Container) CleanupError(fn func(context.Context) error) {
	c.cleanup = append(c.cleanup, fn)
}

// Close calls all cleanup functions
func (c *Container) Close(ctx context.Context) error {
	errs := []error{}
	for _, cleanup := range c.cleanup {
		if err := cleanup(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// DefineContainers defines supplied containers using given config and root container
func DefineContainers[C, CF any](ctx context.Context, cfg CF, definers []func(context.Context, CF, C), root C, containers ...interface {
	Define(context.Context, CF, C)
}) C {
	for _, d := range definers {
		d(ctx, cfg, root)
	}
	for _, d := range containers {
		d.Define(ctx, cfg, root)
	}
	return root
}

// ContainerResolved checks if the container can be resolved.
// It checks as well each instance in the container separately to ensure that all required dependencies are resolved
// and are actually used with resolution function.
func ContainerResolved[C any](containerFunc func() C) error {
	root := containerFunc()
	return containerDependecyResolved(root, containerFunc, []string{})
}

// containerDependecyResolved checks if the particular level in container graph can be resolved.
func containerDependecyResolved[C any](root any, containerFunc func() C, path []string) error {
	errs := []error{}
	v := reflect.ValueOf(root)
	v = reflect.Indirect(v)
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)

		if !field.CanInterface() {
			continue
		}
		var (
			name       = t.Field(i).Name
			actualPath = append(append([]string{}, path...), name)
		)

		tag := t.Field(i).Tag.Get("plumber")
		if tag != "" {
			parts := strings.Split(tag, ",")
			if len(parts) > 1 {
				if parts[1] == "ignore" {
					continue
				}
			}
		}

		it := field.Interface()
		if _, ok := it.(Errorer); ok {
			if err := evaluateDependencyByPath(containerFunc, actualPath); err != nil {
				errs = append(errs, err)
				continue
			}
		} else {
			ptr := reflect.New(field.Type())
			ptr.Elem().Set(field)

			it := ptr.Interface()
			if _, ok := it.(Errorer); ok {
				if err := evaluateDependencyByPath(containerFunc, actualPath); err != nil {
					errs = append(errs, err)
					continue
				}
			}
		}

		field = reflect.Indirect(field)

		if field.Kind() == reflect.Struct {
			if err := containerDependecyResolved(field.Interface(), containerFunc, actualPath); err != nil {
				errs = append(errs, err)
				continue
			}
		}
	}
	return errors.Join(errs...)
}

// evaluateDependencyByPath evaluates dependency by path
func evaluateDependencyByPath[C any](containerFunc func() C, path []string) error {
	errs := []error{}
	dep := ReflectValueByPath(containerFunc, path)
	var errorer Errorer
	if errr, ok := dep.Interface().(Errorer); ok {
		errorer = errr
	} else {
		vl := dep
		if vl.CanAddr() {
			vl = vl.Addr()
			dp := vl.Interface()
			if errr, ok := dp.(Errorer); ok {
				errorer = errr
			}
		}
	}

	if errorer != nil {
		err := errorer.Error()
		path := strings.Join(path, ".")
		if err != nil {
			errs = append(errs, fmt.Errorf("errors on \"%s\": %w", path, err))
		}

		if dep, ok := errorer.(interface{ dependencies() []Dependency }); ok {
			for _, d := range dep.dependencies() {
				if !d.Resolved() {
					errs = append(errs, fmt.Errorf("errors on \"%s\": unused dependency: %s", path, d.String()))
				}
			}
		}
	}
	return errors.Join(errs...)
}

// ReflectValueByPath returns dependency by path
func ReflectValueByPath[C any](containerFunc func() C, path []string) reflect.Value {
	elem := containerFunc()

	if len(path) == 0 {
		return reflect.ValueOf(elem)
	}

	field := reflect.ValueOf(elem)

	for _, name := range path {
		field = reflect.Indirect(field)
		field = field.FieldByName(name)
	}
	return field
}
