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

	NotUsed plumber.D[int32]

	GraphQL plumber.R[*graphql.Server]

	ServerWithNotUsedDep plumber.R[*graphql.Server]

	Server plumber.R[*graphql.Server] `plumber:",ignore"`

	// private should not be accessed and tested
	private plumber.D[int32] // nolint:unused //Why: only for testing
}

// Define resolves dependencies
func (c *Bugs) Define(ctx context.Context, cf *Config, a *Container) {
	c.NotUsed.Named("Port").Const(1)
	c.Notdefined.Named("notdefined")
	c.GraphQL.Named("BuggyGraphQL").Resolve(func(r *plumber.ResolutionR[*graphql.Server]) {
		r.Require(
			&c.Notdefined,
			&c.NotUsed,
		).Then(func() {
			r.ResolveError(graphql.NewServer(
				1,
				nil,
				nil,
			))
		})
	})

	c.ServerWithNotUsedDep.Named("ServerWithNotUsedDep").Resolve(func(r *plumber.ResolutionR[*graphql.Server]) {
		r.Require(
			&c.NotUsed,
		).Then(func() {
			r.ResolveError(graphql.NewServer(
				1,
				nil,
				nil,
			))
		})
	})
}
