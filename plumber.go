// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains dependency management helpers

// Package plumber package provides orchestration library to construct application dependency graph and manage service tasks
package plumber

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// Errors
var (
	// ErrCircularDependency error indicating circular dependency
	ErrCircularDependency = errors.New("circular dependency")

	// ErrRunnerNotDefined error indicates that ResolutionR doesn't have runner set
	ErrRunnerNotDefined = errors.New("runner not defined")
)

// Dependency represent a dependency that can be supplied into Require method
type Dependency interface {
	Iterate(func(dep Dependency) bool)
	Error() error
}

// Future represents a struct that will help with dependency evaluation
type Future[T any] struct {
	d *D[T]
}

// Then evaluates a dependencies and trigger callback when all good
func (f *Future[T]) Then(callback func()) {
	var errs []error
	for _, d := range f.d.deps {
		var (
			circular bool
			err      error
		)
		d.Iterate(func(dep Dependency) bool {
			if f.d == dep {
				circular = true
			}
			return !circular
		})
		if circular {
			err = ErrCircularDependency
		}
		if err == nil {
			err = d.Error()
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("dependency not resolved, %s requires %s (%w)", f.d, d, err))
		}
	}
	if len(errs) != 0 {
		f.d.err = errors.Join(errs...)
		return
	}
	callback()
}

// D represent a dependency wrapper
type D[T any] struct {
	resolving bool
	defined   bool
	resolved  bool
	value     T
	err       error
	once      sync.Once
	mx        sync.Mutex
	resolve   func()
	deps      []Dependency
	listeners []func()
	wrappers  []func(T) T
	name      string
}

// Named creates a new named dependency
func Named[T any](name string) *D[T] {
	var d D[T]
	d.Named(name)
	return &d
}

// Named sets a name for the dependency
func (d *D[T]) Named(name string) *D[T] {
	d.name = name
	return d
}

// String return names of underlaying type
func (d *D[T]) String() string {
	var v T
	s := reflect.TypeOf(&v).Elem().String()
	if d.name == "" {
		return s
	}
	return fmt.Sprintf("%s(%s)", d.name, s)
}

// define sets resolution function but only once
func (d *D[T]) define(resolve func()) {
	d.once.Do(func() {
		d.defined = true
		d.resolve = func() {
			resolve()
			d.resolved = true
			d.resolving = false
			for _, w := range d.wrappers {
				d.value = w(d.value)
			}
			for _, l := range d.listeners {
				l()
			}
		}
	})
}

// Define allows to define value using callback that returns a value and error
func (d *D[T]) DefineError(resolve func() (T, error)) *D[T] {
	d.define(func() {
		d.value, d.err = resolve()
	})
	return d
}

// Define allows to define value using callback that returns a value
func (d *D[T]) Define(resolve func() T) *D[T] {
	d.define(func() {
		d.value = resolve()
	})
	return d
}

// Const assigns a static value
func (d *D[T]) Const(v T) *D[T] {
	d.define(func() {
		d.value = v
	})
	return d
}

// Use overwrites defined value with specific instance. Should be used only for testings
func (d *D[T]) Use(v T) *D[T] {
	d.resolved = true
	d.value = v
	return d
}

// Must returns a value or panics in case of the error
func (d *D[T]) Must() T {
	v, err := d.InstanceError()
	if err != nil {
		panic(err)
	}
	return v
}

// Instance returns a value
func (d *D[T]) Instance() T {
	d.mx.Lock()
	defer d.mx.Unlock()
	var zero T
	if !d.defined {
		return zero
	}
	if d.resolve != nil && !d.resolved {
		d.resolving = true
		d.resolve()
	}
	return d.value
}

// InstanceError returns and a value and the error
func (d *D[T]) InstanceError() (T, error) {
	v := d.Instance()
	err := d.err
	if !d.defined {
		err = fmt.Errorf("instance %s not resolved", d)
	}
	return v, err
}

// Error returns and error
func (d *D[T]) Error() error {
	_, err := d.InstanceError()
	return err
}

// Iterate iterates dependency graph, when callback returns true iterator will continue down stream
func (d *D[T]) Iterate(callback func(dep Dependency) bool) {
	for _, dep := range d.deps {
		if !callback(dep) {
			break
		}
		dep.Iterate(callback)
	}
}

// Resolve returns a callback providing a resolution orchestrator
// Using the orchestrator we can define dependencies between values
func (d *D[T]) Resolve(callback func(*Resolution[T])) *D[T] {
	d.define(func() {
		r := Resolution[T]{d: d}
		callback(&r)
	})
	return d
}

// WhenResolved registers a callback that will be triggered when dependency is resolved
func (d *D[T]) WhenResolved(callback func()) *D[T] {
	d.listeners = append(d.listeners, callback)
	return d
}

// Wrap registers a wrapping callback that will be triggered when dependency is resolved
// The callback allows to augment the original value. Wrapping should be used mostly to
// redefine the dependency for a different test environments
func (d *D[T]) Wrap(wrappers ...func(T) T) *D[T] {
	d.wrappers = append(d.wrappers, wrappers...)
	return d
}

// R represents a runnable dependency wrapper
// It is meant to be supplied into the Pipeline()
type R[T any] struct {
	D[T]
	runnable Runner
}

// NamedR creates a new named runnable dependency
func NamedR[T any](name string) *R[T] {
	var r R[T]
	r.Named(name)
	return &r
}

// Named sets a name for the dependency
func (r *R[T]) Named(name string) *R[T] {
	r.D.name = name
	return r
}

// Resolve returns a callback providing a resolution orchestrator
// Using the orchestrator we can define dependencies between values
func (r *R[T]) Resolve(callback func(*ResolutionR[T])) *R[T] {
	r.D.Resolve(func(dr *Resolution[T]) {
		rr := &ResolutionR[T]{resolution: dr, r: r}
		callback(rr)
	})
	return r
}

// Define allows to define value using callback that returns a value
// given instance must by a runnable
func (r *R[T]) Define(resolve func() T) *R[T] {
	r.D.DefineError(func() (T, error) {
		var empty T
		rv := resolve()
		var v any = rv
		if runner, ok := v.(Runner); ok {
			r.runnable = runner
		} else {
			return empty, errors.New("instance is not a runnable")
		}
		return rv, nil
	})
	return r
}

// DefineError to define value using callback that returns a value and error
// given instance must by a runnable
func (r *R[T]) DefineError(resolve func() (T, error)) *R[T] {
	r.D.DefineError(func() (T, error) {
		rv, err := resolve()
		var v any = rv
		if runner, ok := v.(Runner); ok {
			r.runnable = runner
		}
		return rv, err
	})
	return r
}

// Run executes Run method on value and satisfies Runner,Closer and Readier interfaces
func (r *R[T]) Run(ctx context.Context) error {
	if err := r.D.Error(); err != nil {
		return err
	}
	if r.runnable == nil {
		return fmt.Errorf("Runnable %s not resolved", &r.D)
	}
	return r.runnable.Run(ctx)
}

// Error returns an error
func (r *R[T]) Error() error {
	if err := r.D.Error(); err != nil {
		return err
	}
	if r.runnable == nil {
		return fmt.Errorf("Runnable %s not resolved", &r.D)
	}
	return nil
}

// Close executes Close method on value and satisfies Closer interface
func (r *R[T]) Close(ctx context.Context) error {
	if err := r.D.Error(); err != nil {
		return err
	}
	if r.runnable == nil {
		return fmt.Errorf("Runnable %s not resolved", &r.D)
	}
	return RunnerClose(ctx, r.runnable)
}

func (r *R[T]) Ready() (<-chan struct{}, error) {
	if err := r.D.Error(); err != nil {
		return nil, err
	}
	if r.runnable == nil {
		return nil, fmt.Errorf("Runnable %s not resolved", &r.D)
	}
	return RunnerReady(r.runnable)
}

// Resolution is value resolution orchestrator
type Resolution[T any] struct {
	d *D[T]
}

// Resolved ends the resolution with given value
func (r *Resolution[T]) Resolve(v T) {
	r.d.value = v
}

// Error ends resolution with and error
func (r *Resolution[T]) Error(err error) {
	r.d.err = err
}

// ResolveError ends the resolution with given value and error
func (r *Resolution[T]) ResolveError(v T, err error) {
	r.Resolve(v)
	r.Error(err)
}

// Require allows to define a dependant for the current value
// It is a necessary to call Then to trigger a dependency evaluation
func (r *Resolution[T]) Require(deps ...Dependency) *Future[T] {
	r.d.deps = deps
	return &Future[T]{
		d: r.d,
	}
}

// ResolutionR represents a resolution orchestrator for a runnable values
type ResolutionR[T any] struct {
	r          *R[T]
	resolution *Resolution[T]
}

// Error ends resolution with and error
func (rr *ResolutionR[T]) Error(err error) {
	rr.resolution.Error(err)
}

// Resolved ends the resolution with given runnable value
// This instance will be executed once a R included int the started pipeline
func (rr *ResolutionR[T]) Resolve(v Runner) {
	rr.resolution.Resolve(v.(T))
	rr.r.runnable = v
}

// ResolveError ends the resolution with given value and error
func (rr *ResolutionR[T]) ResolveError(v Runner, err error) {
	rr.Resolve(v)
	rr.Error(err)
}

// ResolveAdapter ends the resolution with given value and runnable adapter
// that will be executed once a R is included int the started pipeline
func (rr *ResolutionR[T]) ResolveAdapter(v T, runnable Runner) {
	rr.resolution.Resolve(v)
	rr.r.runnable = runnable
}

// Require allows to define a dependant for the current value
// It is a necessary to call Then to trigger a dependency evaluation
func (rr *ResolutionR[T]) Require(deps ...Dependency) *Future[T] {
	return rr.resolution.Require(deps...)
}

// Resolved checks given dependencies and checks whether they are resolved or not.
// Multi error is returned.
func Resolved(deps ...interface{ Error() error }) error {
	var errs []error
	for _, d := range deps {
		if err := d.Error(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
