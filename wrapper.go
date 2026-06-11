// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains the implementation of the Wrapper interface and related helper
// functions to define wrappers for dependencies in the Plumber library.
package plumber

// Wrapper represents a wrapper for a dependency. It allows to define a wrapper function that will be applied to the resolved value
// of the dependency.
// The wrapper can also return an error, in which case the resolution will fail with that error.
type Wrapper[T any] interface {
	InstanceError() (func(T) T, error)
	Instance() func(T) T
}

// WrapperFunc is a helper function to create a simple wrapper without error handling
func WrapperFunc[T any](wrapper func(T) T) ProxyWrapper[T] {
	return ProxyWrapper[T]{
		wrapper: func(a T) T {
			wrapper(a)
			return a
		},
	}
}

// WrapperFuncError is a helper function to create a wrapper with error handling
// The wrapper will be applied only if errFunc returns nil error
func WrapperFuncError[T any](wrapper func(T) T, errFunc func() error) ProxyWrapper[T] {
	return ProxyWrapper[T]{
		wrapper: func(a T) T {
			wrapper(a)
			return a
		},
	}
}

// ProxyWrapper is a simple implementation of Wrapper interface that allows to define a wrapper function and an error function
type ProxyWrapper[T any] struct {
	wrapper func(T) T
	errFunc func() error
}

func (w ProxyWrapper[T]) Instance() func(T) T {
	return w.wrapper
}

func (w ProxyWrapper[T]) Error() error {
	if w.errFunc != nil {
		return w.errFunc()
	}
	return nil
}

func (w ProxyWrapper[T]) InstanceError() (func(T) T, error) {
	return w.wrapper, w.Error()
}
