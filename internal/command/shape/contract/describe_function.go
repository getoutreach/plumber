package contract

import "text/template"

type FunctionDescriptor[T any] struct {
	Description FunctionDescription
	Func        func(T) any
}

type FunctionDescription struct {
	Name        string
	Description string
	Usage       string
}

// Void context can be used when no context is needed for the function.
type VoidContext struct{}

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

type FunctionDescriptions interface {
	Descriptions() []FunctionDescription
}
