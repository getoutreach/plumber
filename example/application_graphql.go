// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: graphql related dependencies
package example

import (
	"context"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/adapter/graphql"
)

// GraphQL represents graphql related dependency container
type GraphQL struct {
	Port   plumber.D[int32]
	Server plumber.R[*graphql.Server]
}

// Define resolves dependencies
func (c *GraphQL) Define(ctx context.Context, cf *Config, a *Container) {
	c.Port.Const(5000)

	c.Server.Resolve(func(r *plumber.ResolutionR[*graphql.Server]) {
		r.Require(
			&a.Service.Querier,
			&a.Service.Mutator,
		).Then(func() {
			r.ResolveError(graphql.NewServer(
				c.Port.Instance(),
				a.Service.Querier.Instance(),
				a.Service.Mutator.Instance(),
			))
		})
	})
}
