// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Function descriptions and builders for template functions.
package contract

import "text/template"

// FunctionDescriptor describes a template function, including its name, description, usage, and the actual function implementation.
type FunctionDescriptor[T any] struct {
	Description FunctionDescription
	Func        func(T) any
}

// FunctionDescription provides metadata about a template function, such as its name,
// a description of what it does, and an example of how to use it.
type FunctionDescription struct {
	Name        string
	Description string
	Usage       string
}

// VoidContext can be used when no context is needed for the function.
type VoidContext struct{}

// FunctionDescriptors is a collection of FunctionDescriptor, which can be converted to a template.FuncMap for use in templates.
type FunctionDescriptors[T any] []FunctionDescriptor[T]

func (fds FunctionDescriptors[T]) ToMap(f T) template.FuncMap {
	m := make(template.FuncMap)
	for _, fd := range fds {
		m[fd.Description.Name] = fd.Func(f)
	}
	return m
}

// Dispose is a no-op for now, but can be used to clean up any resources if needed in the future.
// Importantly, it keeps context within scope so the so it survives for the duration of the render.
func (fds FunctionDescriptors[T]) Dispose(T) func() {
	return func() {
		// Implement disposal logic if needed
	}
}

func (fds FunctionDescriptors[T]) Descriptions() []FunctionDescription {
	descs := make([]FunctionDescription, len(fds))
	for i, fd := range fds {
		descs[i] = fd.Description
	}
	return descs
}

// FunctionDescriptions is an interface that provides a method to retrieve a slice of FunctionDescription,
// allowing for introspection of available template functions.
type FunctionDescriptions interface {
	Descriptions() []FunctionDescription
}
