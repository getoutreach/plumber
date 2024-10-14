// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains root dependency container

package plumber

import (
	"context"
	"errors"
)

// Container represents a root dependency container
type Container struct {
	cleanup []func() error
}

// Cleanup registers a cleanup function
func (c *Container) Cleanup(fn func()) {
	c.cleanup = append(c.cleanup, func() error {
		fn()
		return nil
	})
}

// CleanupError registers a cleanup function returning an error
func (c *Container) CleanupError(fn func() error) {
	c.cleanup = append(c.cleanup, fn)
}

// Close calls all cleanup functions
func (c *Container) Close() error {
	errs := []error{}
	for _, cleanup := range c.cleanup {
		if err := cleanup(); err != nil {
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
