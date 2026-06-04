// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Function descriptions and builders for template functions.
package contract

import (
	"reflect"
	"text/template"
)

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

// FunctionSignature pairs a function description with the runtime
// reflect.Type information of its underlying implementation. It enables
// introspection of parameter and result types for documentation purposes.
type FunctionSignature struct {
	Description FunctionDescription
	ParamTypes  []reflect.Type
	Variadic    bool
	ResultTypes []reflect.Type
}

// FunctionSignaturesProvider exposes runtime signature information for a
// collection of template functions.
type FunctionSignaturesProvider interface {
	Signatures() []FunctionSignature
}

// FunctionSource combines descriptive metadata with runtime signature
// information for a collection of template functions.
type FunctionSource interface {
	FunctionDescriptions
	FunctionSignaturesProvider
}

// Signatures returns the runtime signature information for each
// FunctionDescriptor in the collection. The underlying function is
// instantiated against a zero value of T solely to obtain its
// reflect.Type; if a constructor panics on a zero context, an empty
// signature (no params/results) is recorded for that function.
func (fds FunctionDescriptors[T]) Signatures() []FunctionSignature {
	var zero T
	out := make([]FunctionSignature, 0, len(fds))
	for _, fd := range fds {
		out = append(out, signatureOf(fd, zero))
	}
	return out
}

// signatureOf builds the FunctionSignature for a single FunctionDescriptor,
// guarding against panics during closure construction.
func signatureOf[T any](fd FunctionDescriptor[T], zero T) (sig FunctionSignature) {
	sig.Description = fd.Description
	defer func() {
		if r := recover(); r != nil {
			sig.Variadic = false
			sig.ParamTypes = nil
			sig.ResultTypes = nil
		}
	}()
	fn := fd.Func(zero)
	if fn == nil {
		return sig
	}
	rt := reflect.TypeOf(fn)
	if rt == nil || rt.Kind() != reflect.Func {
		return sig
	}
	sig.Variadic = rt.IsVariadic()
	sig.ParamTypes = make([]reflect.Type, rt.NumIn())
	for i := 0; i < rt.NumIn(); i++ {
		sig.ParamTypes[i] = rt.In(i)
	}
	sig.ResultTypes = make([]reflect.Type, rt.NumOut())
	for i := 0; i < rt.NumOut(); i++ {
		sig.ResultTypes[i] = rt.Out(i)
	}
	return sig
}
