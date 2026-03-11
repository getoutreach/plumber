// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: async related dependencies
package example

import (
	"context"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/discovery"
	"github.com/getoutreach/plumber/example/adapter/async"
)

// Async service represents async processing related dependency container
type Async struct {
	Publisher plumber.D[*async.Publisher]
}

// Define resolves dependencies
func (c *Async) Define(ctx context.Context, cf *Config, a *Container) {
	c.Publisher.Resolver(func(r *plumber.Resolution[*async.Publisher]) {
		r.Require().Then(func() {
			r.Resolve(async.NewPublisher(discovery.Undefined[string]()))
		})
	})
}
