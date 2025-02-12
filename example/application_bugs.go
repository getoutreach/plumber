// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: async related dependencies
package example

import (
	"context"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/adapter/async"
	"github.com/getoutreach/plumber/example/adapter/graphql"
)

// Bugs service represents container with broken dependencies for testing
type Bugs struct {
	Notdefined plumber.R[*async.Publisher]

	GraphQL plumber.R[*graphql.Server]

	Server plumber.R[*graphql.Server] `plumber:",ignore"`

	// private should not be accessed and tested
	private plumber.D[int32] // nolint:unused //Why: only for testing
}

// Define resolves dependencies
func (c *Bugs) Define(ctx context.Context, cf *Config, a *Container) {
	c.Notdefined.Named("notdefined")
	c.GraphQL.Resolve(func(r *plumber.ResolutionR[*graphql.Server]) {
		r.Require(
			&c.Notdefined,
		).Then(func() {
			r.ResolveError(graphql.NewServer(
				1,
				nil,
				nil,
			))
		})
	})
}
