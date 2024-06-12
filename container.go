// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: This file contains root dependency container

package plumber

import "errors"

// Container represents a root dependency container
type Container struct {
	cleanup []func() error
}

// Cleanup registers a cleanup function
func (c *Container) Cleanup(fn func() error) {
	c.cleanup = append(c.cleanup, fn)
}

// Close calls all clenaup functions
func (c *Container) Close() error {
	errs := []error{}
	for _, cleanup := range c.cleanup {
		if err := cleanup(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
