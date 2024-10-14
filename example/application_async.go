// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: async related dependencies
package example

import (
	"context"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/adapter/async"
)

// Async service represents async processing related dependency container
type Async struct {
	Publisher plumber.R[*async.Publisher]
}

// Define resolves dependencies
func (c *Async) Define(ctx context.Context, cf *Config, a *Container) {
	c.Publisher.Resolve(func(rr *plumber.ResolutionR[*async.Publisher]) {
		rr.Resolve(async.NewPublisher(cf.AsyncBroker))
	})
}
