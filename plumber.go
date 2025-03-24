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
	String() string
	Resolved() bool
	Iterate(func(dep Dependency) bool)
	Error() error
}

// dependencyErrorer represents an interface that can return an error
type dependencyErrorer interface {
	dependencyErrors() []error
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
			errs = append(errs, fmt.Errorf("dependency not resolved, %s requires %s (%w)", f.d.String(), d.String(), err))
		}
	}
	if len(errs) != 0 {
		f.d.err = errors.Join(errs...)
		return
	}
	if f.d.futureCallback != nil {
		f.d.futureCallback(callback)
	} else {
		callback()
	}
}

// D represent a dependency wrapper
type D[T any] struct {
	resolving         bool
	defined           bool
	instanceErrorFunc func() (T, error)
	resolved          bool
	value             T
	err               error
	once              sync.Once
	mx                sync.Mutex
	depMx             sync.RWMutex
	resolve           func()
	deps              []Dependency
	listeners         []func()
	wrappers          []func(T) T
	name              string
	instanceRetrieved func()
	resolution        *Resolution[T]
	futureCallback    func(next func())
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
func (d *D[T]) define(resolve func(), callbacks ...func()) {
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
			for _, c := range callbacks {
				c()
			}
		}
	})
}

// Define allows to define value using callback that returns a value and error
func (d *D[T]) DefineError(resolve func() (T, error)) *D[T] {
	d.instanceErrorFunc = resolve
	d.define(func() {
		d.value, d.err = resolve()
	})
	return d
}

// Define allows to define value using callback that returns a value
func (d *D[T]) Define(resolve func() T) *D[T] {
	d.instanceErrorFunc = func() (T, error) {
		return resolve(), nil
	}
	d.define(func() {
		d.value = resolve()
	})
	return d
}

// Const assigns a static value
func (d *D[T]) Const(v T) *D[T] {
	d.instanceErrorFunc = func() (T, error) {
		return v, nil
	}
	d.define(func() {
		d.value = v
	})
	return d
}

// Use overwrites defined value with specific instance. Should be used only for testings
func (d *D[T]) Use(v T) *D[T] {
	d.instanceErrorFunc = func() (T, error) {
		return v, nil
	}
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
	if d.instanceRetrieved != nil {
		d.instanceRetrieved()
	}
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
		err = fmt.Errorf("instance %s not resolved", d.String())
	}
	return v, err
}

// Error returns and error
func (d *D[T]) Error() error {
	_, err := d.InstanceError()
	return err
}

// MakeInstanceError builds new instance and returns a value and an error
func (d *D[T]) MakeInstanceError() (T, error) {
	return d.instanceErrorFunc()
}

// MakeInstanceError builds new instance and returns a value if error occurs it panics
func (d *D[T]) MakeInstance() T {
	instance, err := d.instanceErrorFunc()
	if err != nil {
		panic(err)
	}
	return instance
}

// setInstanceListener sets a listener that will be triggered when instance is retrieved
func (d *D[T]) setInstanceListener(listener func()) {
	d.instanceRetrieved = listener
}

// dependencyErrors returns a list during dependency resolution
func (d *D[T]) dependencyErrors() []error {
	errs := []error{}
	d.futureCallback = func(next func()) {
		definedDependencies := map[Dependency]struct{}{}
		for _, dep := range d.deps {
			definedDependencies[dep] = struct{}{}
			if depListener, ok := dep.(interface{ setInstanceListener(func()) }); ok {
				depListener.setInstanceListener(func() {
					delete(definedDependencies, dep)
				})
			}
		}
		next()
		for d := range definedDependencies {
			errs = append(errs, fmt.Errorf("dependency declared but not used: %s", d))
		}
	}
	err := d.Error()
	if err != nil {
		errs = append(errs, err)
	}
	return errs
}

// Resolved returns true if dependency is resolved
func (d *D[T]) Resolved() bool {
	return d.resolved
}

// Iterate iterates dependency graph, when callback returns true iterator will continue down stream
func (d *D[T]) Iterate(callback func(dep Dependency) bool) {
	d.depMx.RLock()
	defer d.depMx.RUnlock()

	deps := d.deps
	for _, dep := range deps {
		if !callback(dep) {
			break
		}
		dep.Iterate(callback)
	}
}

// Deprecated: Use Resolver instead
func (d *D[T]) Resolve(callback func(*Resolution[T])) *D[T] {
	return d.Resolver(callback)
}

// Resolver returns a callback providing a resolution orchestrator
// Using the orchestrator we can define dependencies between values
func (d *D[T]) Resolver(callback func(*Resolution[T])) *D[T] {
	r := Resolution[T]{
		setInstance: func(v T) {
			d.value = v
		},
		setError: func(err error) {
			d.err = err
		},
		d: d,
	}
	d.instanceErrorFunc = func() (value T, err error) {
		localR := Resolution[T]{
			setInstance: func(v T) {
				value = v
			},
			setError: func(e error) {
				err = e
			},
			d: d,
		}
		callback(&localR)
		return value, err
	}
	d.define(func() {
		callback(&r)
	}, func() {
		d.resolution = &r
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
	d        D[T]
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
	r.d.Named(name)
	return r
}

// Deprecated: use Resolver instead
func (r *R[T]) Resolve(callback func(*ResolutionR[T])) *R[T] {
	return r.Resolver(callback)
}

// Resolve returns a callback providing a resolution orchestrator
// Using the orchestrator we can define dependencies between values
func (r *R[T]) Resolver(callback func(*ResolutionR[T])) *R[T] {
	r.d.Resolver(func(dr *Resolution[T]) {
		rr := &ResolutionR[T]{resolution: dr, r: r}
		callback(rr)
	})
	return r
}

// Instance returns a value
func (r *R[T]) Instance() T {
	return r.d.Instance()
}

// String return names of underlaying type
func (r *R[T]) String() string {
	return r.d.String()
}

// Resolved returns true if dependency is resolved
func (r *R[T]) Resolved() bool {
	return r.d.Resolved()
}

// dependencyErrors returns a list during dependency resolution
func (r *R[T]) dependencyErrors() []error {
	return r.d.dependencyErrors()
}

func (r *R[T]) setInstanceListener(listener func()) {
	r.d.setInstanceListener(listener)
}

// InstanceError returns and a value and the error
func (r *R[T]) InstanceError() (T, error) {
	v := r.d.Instance()
	err := r.d.err
	if err == nil {
		err = r.Error()
	}
	return v, err
}

// Define allows to define value using callback that returns a value
// given instance must by a runnable
func (r *R[T]) Define(resolve func() T) *R[T] {
	r.d.DefineError(func() (T, error) {
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
	r.d.DefineError(func() (T, error) {
		var empty T
		rv, err := resolve()
		var v any = rv
		if runner, ok := v.(Runner); ok {
			r.runnable = runner
		} else {
			return empty, errors.New("instance is not a runnable")
		}
		return rv, err
	})
	return r
}

// Run executes Run method on value and satisfies Runner,Closer and Readier interfaces
func (r *R[T]) Run(ctx context.Context) error {
	if err := r.d.Error(); err != nil {
		return err
	}
	if r.runnable == nil {
		return fmt.Errorf("Runnable %s not resolved 1", &r.d)
	}
	return r.runnable.Run(ctx)
}

// Error returns an error
func (r *R[T]) Error() error {
	if err := r.d.Error(); err != nil {
		return err
	}
	if r.runnable == nil {
		return fmt.Errorf("Runnable %s not resolved 2", &r.d)
	}
	return nil
}

// Iterate iterates dependency graph, when callback returns true iterator will continue down stream
func (r *R[T]) Iterate(callback func(dep Dependency) bool) {
	r.d.Iterate(callback)
}

// Close executes Close method on value and satisfies Closer interface
func (r *R[T]) Close(ctx context.Context) error {
	if err := r.d.Error(); err != nil {
		return err
	}
	if r.runnable == nil {
		return fmt.Errorf("Runnable %s not resolved 3", &r.d)
	}
	return RunnerClose(ctx, r.runnable)
}

func (r *R[T]) Ready() <-chan struct{} {
	if err := r.d.Error(); err != nil {
		return nil
	}
	return RunnerReady(r.runnable)
}

// Resolution is value resolution orchestrator
type Resolution[T any] struct {
	setInstance func(T)
	setError    func(error)
	d           *D[T]
	f           *Future[T]
}

// Resolved ends the resolution with given value
func (r *Resolution[T]) Resolve(v T) {
	r.setInstance(v)
}

// Error ends resolution with and error
func (r *Resolution[T]) Error(err error) {
	r.setError(err)
}

// ResolveError ends the resolution with given value and error
func (r *Resolution[T]) ResolveError(v T, err error) {
	r.Resolve(v)
	r.Error(err)
}

// Require allows to define a dependant for the current value
// It is a necessary to call Then to trigger a dependency evaluation
func (r *Resolution[T]) Require(deps ...Dependency) *Future[T] {
	r.d.depMx.Lock()
	r.d.deps = deps
	r.d.depMx.Unlock()
	f := &Future[T]{
		d: r.d,
	}
	r.f = f
	return f
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
