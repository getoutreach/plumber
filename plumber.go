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

// resettable is an internal interface for dependencies that can be reset.
// Used by Reset and Redefine to clear cached state.
type resettable interface {
	// resetResolved clears the resolved cache (value, err, resolved flag)
	// but keeps the definition intact so re-resolution uses the same definition.
	resetResolved()
	// resetFull clears everything including sync.Once and defined flag,
	// allowing the dependency to accept a new Define/Const/Resolver call.
	resetDefined()
	// directDeps returns the direct dependencies declared via Require.
	directDeps() []Dependency
}

// savepointable is an internal interface for dependencies that can create savepoints of their state.
type savepointable interface {
	savePoint() func()
}

// Future represents a struct that will help with dependency evaluation
type Future[T any] struct {
	d *D[T]
}

// Then evaluates a dependencies and trigger callback when all good
func (f *Future[T]) Then(callback func()) {
	// While probing for dependency discovery, Require has already populated
	// f.d.deps; we must not invoke the user callback (and thus must not touch
	// any of the required dependencies, which may themselves still be
	// unresolved).
	if f.d.probing {
		return
	}
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
		f.d.state.err = errors.Join(errs...)
		return
	}
	if f.d.futureCallback != nil {
		f.d.futureCallback(callback)
	} else {
		callback()
	}
}

// state represents the internal state of a dependency, including whether it's defined,
// resolved, its value, and any error encountered during resolution.
type state[T any] struct {
	// resolved indicates whether the dependency has been resolved and its value is ready to be used.
	resolved bool
	// resolve is the function that performs the resolution of the dependency.
	resolve func()
	// defined indicates whether the dependency has been defined (via Define/Const/Resolver).
	defined bool
	// value holds the resolved value of the dependency.
	value T
	// err holds any error encountered during the resolution of the dependency.
	err error
	// resolution holds the Resolution struct if the dependency was defined via Resolver,
	// which allows for re-probing dependencies on ResetDefinition.
	resolution *Resolution[T]

	// wrappers holds the list of wrappers that should be applied to the resolved value before it's returned by Instance.
	wrappers []Wrapper[T]
}

// D represent a dependency wrapper
type D[T any] struct {
	// state holds the internal state of the dependency, including whether it's defined,
	// resolved, its value, and any error encountered during resolution.
	state state[T]
	// resolving is set to true while the resolver is being invoked in resolution mode (see Resolver). While resolving,
	resolving bool
	//defined   bool
	// probing is set to true while the resolver is being invoked in
	// dependency-discovery mode (see probeDependencies). While probing,
	// Future.Then is a no-op so the resolver body executes only far enough
	// to register Require() calls.
	probing bool

	// makeInstanceErrorFunc is the original resolution function provided by Define/Const/Resolver.
	// it is used to re-evaluate the instance when ResetDefinition is called,
	// and is also used by wrappers to get the original value.
	makeInstanceErrorFunc func() (T, error)
	//resolved          bool
	//value             T
	//err               error
	once  sync.Once
	mx    sync.Mutex
	depMx sync.RWMutex
	//resolve           func()
	deps              []Dependency
	listeners         []func()
	name              string
	instanceRetrieved func()
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
		d.state.defined = true
		d.state.resolve = func() {
			resolve()
			d.state.resolved = true
			d.resolving = false

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
func (d *D[T]) DefineError(resolve func() (T, error)) {
	d.makeInstanceErrorFunc = resolve
	d.define(func() {
		d.state.value, d.state.err = resolve()
	})
}

// Define allows to define value using callback that returns a value
func (d *D[T]) Define(resolve func() T) {
	d.makeInstanceErrorFunc = func() (T, error) {
		return resolve(), nil
	}
	d.define(func() {
		d.state.value = resolve()
	})
}

// Const assigns a static value
func (d *D[T]) Const(v T) {
	d.makeInstanceErrorFunc = func() (T, error) {
		return v, nil
	}
	d.define(func() {
		d.state.value = v
	})
}

// Use overwrites defined value with specific instance. Should be used only for testings
func (d *D[T]) Use(v T) *D[T] {
	d.makeInstanceErrorFunc = func() (T, error) {
		return v, nil
	}
	d.state.resolved = true
	d.state.value = v
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

// Instance returns a value or panics if error occurs during resolution
// To avoid panics use InstanceError that returns value and error separately but
// as well don't forget to declare dependencies using Resolver if instance requires other dependencies to be resolved first
func (d *D[T]) Instance() T {
	i, err := d.InstanceError()
	if err != nil {
		panic(err)
	}
	return i
}

// InstanceError returns and a value and the error
func (d *D[T]) InstanceError() (T, error) {
	d.mx.Lock()
	defer d.mx.Unlock()

	if d.instanceRetrieved != nil {
		d.instanceRetrieved()
	}
	var zero T

	if err := d.checkDefined(); err != nil {
		return zero, err
	}

	if d.state.resolve != nil && !d.state.resolved {
		d.resolving = true
		d.state.resolve()
	}

	// Apply wrappers in the order they were registered. Each wrapper can modify the instance and return an error.
	i := d.state.value
	for _, w := range d.state.wrappers {
		wrapperFunc, err := w.InstanceError()
		if err != nil {
			return zero, err
		}
		i = wrapperFunc(i)
	}

	return i, d.state.err
}

// Error returns and error
func (d *D[T]) Error() error {
	_, err := d.InstanceError()
	return err
}

// InstanceErrorWrap returns a value and the error, applying wrappers on the spot.
// Could be used when it is required to defined wrappers on the receiving side.
func (d *D[T]) InstanceErrorWrap(wrappers ...Wrapper[T]) (T, error) {
	i := d.InstanceWrap(wrappers...)
	for _, w := range wrappers {
		wrapperFunc, err := w.InstanceError()
		if err != nil {
			var zero T
			return zero, err
		}
		i = wrapperFunc(i)
	}
	return i, d.Error()
}

// InstanceWrap returns a value, applying wrappers on the spot.
// Could be used when it is required to defined wrappers on the receiving side.
// It is expected that wrappers would be defined as dependency so the we don't need to handle error.
// Panics if error occurs during wrapper evaluation.
func (d *D[T]) InstanceWrap(wrappers ...Wrapper[T]) T {
	i, err := d.InstanceErrorWrap(wrappers...)
	if err != nil {
		panic(fmt.Errorf("undeclared dependency caused unhandled dependency error during instance wrapping: %w", err))
	}
	return i
}

func (d *D[T]) checkDefined() error {
	if !d.state.defined {
		return fmt.Errorf("instance %s not resolved", d.String())
	}
	return nil
}

// MakeInstanceError builds new instance and returns a value and an error
func (d *D[T]) MakeInstanceError() (T, error) {
	var zero T
	if err := d.checkDefined(); err != nil {
		return zero, err
	}
	if d.makeInstanceErrorFunc == nil {
		return zero, errors.New("no resolver or const defined")
	}
	return d.makeInstanceErrorFunc()
}

// MakeInstance builds new instance and returns a value if error occurs it panics
func (d *D[T]) MakeInstance() T {
	instance, err := d.MakeInstanceError()
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
	return d.state.resolved
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

// resetResolved clears the resolved cache so that the next Instance() call
// re-triggers the existing resolve function.
func (d *D[T]) resetResolved() {
	d.mx.Lock()
	defer d.mx.Unlock()
	d.state.resolved = false
	d.resolving = false
	var zero T

	d.state.value = zero
	d.state.err = nil
	d.once = sync.Once{} // Allow the define() logic to run again on the next Instance() call.
	// If there is an existing resolution, re-register it to ensure it uses the current state and dependencies.
	if d.state.resolution != nil {
		d.Resolver(d.state.resolution.callback)
	}
}

// savePoint creates a save point of the current state and returns a restore function that can be called to revert to that state.
func (d *D[T]) savePoint() func() {
	d.mx.Lock()
	defer d.mx.Unlock()
	state := d.state
	return func() {
		d.state = state
		if !d.state.defined {
			d.once = sync.Once{} // Reset once if the restored state is not defined, allowing define() to run again.
		}
	}
}

// resetFull clears everything including sync.Once and defined flag,
// allowing the dependency to be fully re-defined.
func (d *D[T]) resetDefined() {
	d.mx.Lock()
	defer d.mx.Unlock()
	d.state.defined = false
}

// directDeps returns the direct dependencies declared via Require.
func (d *D[T]) directDeps() []Dependency {
	d.depMx.RLock()
	defer d.depMx.RUnlock()
	return d.deps
}

// Deprecated: Use Resolver instead
func (d *D[T]) Resolve(callback func(*Resolution[T])) *D[T] {
	return d.Resolver(callback)
}

// Resolver returns a callback providing a resolution orchestrator
// Using the orchestrator we can define dependencies between values
func (d *D[T]) Resolver(callback func(*Resolution[T])) *D[T] {
	r := &Resolution[T]{
		callback: callback,
		d:        d,
	}
	r.setInstance = func(v T) {
		d.state.value = v
	}
	r.setError = func(err error) {
		d.state.err = err
	}
	d.makeInstanceErrorFunc = func() (value T, err error) {
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
		callback(r)
	}, func() {
		d.state.resolution = r
	})
	// Eagerly probe the resolver to discover declared dependencies (via Require)
	// without actually resolving the value. This ensures that ResetDefinition
	// can cascade resets to all dependents even when those dependents have not
	// yet been resolved through a call to Instance().
	d.probeDependencies(callback)
	return d
}

// probeDependencies invokes the user-supplied resolver callback with a
// "probing" Resolution. The probe records any dependencies declared via
// Require but suppresses any further side effects: setInstance, setError,
// and the Future.Then callback are all turned into no-ops. The probe is
// guarded by the d.probing flag which is read by Future.Then.
func (d *D[T]) probeDependencies(callback func(*Resolution[T])) {
	d.probing = true
	defer func() {
		d.probing = false
		// Probing must never leave an error behind.
		d.state.err = nil
	}()
	probe := &Resolution[T]{
		callback:    callback,
		setInstance: func(T) {},
		setError:    func(error) {},
		d:           d,
	}
	callback(probe)
}

// WhenResolved registers a callback that will be triggered when dependency is resolved
func (d *D[T]) WhenResolved(callback func()) *D[T] {
	d.listeners = append(d.listeners, callback)
	return d
}

// Wrap registers a wrapping callback that will be triggered when dependency is resolved
// The callback allows to augment the original value. Wrapping should be used mostly to
// redefine the dependency for a different test environments
func (d *D[T]) Wrap(wrappers ...Wrapper[T]) *D[T] {
	d.mx.Lock()
	defer d.mx.Unlock()
	d.state.wrappers = append(d.state.wrappers, wrappers...)
	return d
}

// As allows to reuse parent dependency definition and apply different wrappers based on the environment or other conditions.
// It creates a new instance of same kind
func (d *D[T]) As(parent *D[T]) *D[T] {
	d.makeInstanceErrorFunc = parent.MakeInstanceError
	d.define(func() {
		d.state.value, d.state.err = parent.MakeInstanceError()
	})
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
	err := r.d.state.err
	if err == nil {
		err = r.Error()
	}
	return v, err
}

// MakeInstanceError builds new instance and returns a value and an error
func (r *R[T]) MakeInstanceError() (T, error) {
	return r.d.MakeInstanceError()
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

// resetResolved clears the resolved cache on the inner D[T] and nils the runnable.
func (r *R[T]) resetResolved() {
	r.d.resetResolved()
	r.runnable = nil
}

func (r *R[T]) savePoint() func() {
	return r.d.savePoint()
}

// resetFull clears everything including sync.Once and defined flag on the inner D[T].
func (r *R[T]) resetDefined() {
	r.d.resetDefined()
	r.runnable = nil
}

// directDeps returns the direct dependencies declared via Require.
func (r *R[T]) directDeps() []Dependency {
	return r.d.directDeps()
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

// Wrap registers a wrapping callback that will be triggered when dependency is resolved
// The order of wrappers is from first to last, so the first wrapper will be the closest to the original value.
// Latest wrapper will be the evaluated first and will be the closest to the caller.
//
// value.Wrap(w1, w2, w3) will result in w3(w2(w1(value)))
func (r *R[T]) Wrap(wrappers ...Wrapper[T]) *R[T] {
	r.d.Wrap(wrappers...)
	return r
}

// As allows to reuse parent dependency definition and apply different wrappers based on the environment or other conditions.
func (r *R[T]) As(parent *R[T]) *R[T] {
	r.d.As(&parent.d)
	return r
}

// Resolution is value resolution orchestrator
type Resolution[T any] struct {
	callback    func(*Resolution[T])
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
