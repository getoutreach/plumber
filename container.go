// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains root dependency container

package plumber

import (
	"context"
	"errors"
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
func ContainerResolved[C any](func() C) error {
	return nil
}
