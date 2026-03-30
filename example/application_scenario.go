// Copyright 2025 Outreach Corporation. All Rights Reserved.
// Description: Scenario related dependencies example
package example

import (
	"context"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/scenario"
)

// Scenario dependency container
type Scenario struct {
	Scenario plumber.D[*scenario.Scenario]
}

// Define dependency resolvers
func (c *Scenario) Define(ctx context.Context, cf *Config, a *Container) {

	c.Scenario.Resolver(func(r *plumber.Resolution[*scenario.Scenario]) {
		r.Require(
			&a.Async.Publisher,
		).Then(func() {
			r.Resolve(scenario.NewScenario(
				a.Async.Publisher.Instance(),
			))
		})
	})
}
