// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: grpc related dependencies
package example

import (
	"context"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/adapter/grpc"
)

// Grpc service represents grpc related dependency container
type Grpc struct {
	Port   plumber.D[int32]
	Server plumber.R[*grpc.Server]
}

// Define resolves dependencies
func (c *Grpc) Define(ctx context.Context, cf *Config, a *Container) {
	c.Port.Const(5000)

	c.Server.Resolver(func(r *plumber.ResolutionR[*grpc.Server]) {
		r.Require(
			&c.Port,
			&a.Service.Querier,
			&a.Service.NotifyingMutator,
			&c.Port,
			&a.Service.Querier,
			&a.Service.NotifyingMutator,
			&c.Port,
			&a.Service.Querier,
			&a.Service.NotifyingMutator,
		).Then(func() {
			r.ResolveError(grpc.NewServer(
				c.Port.Instance(),
				a.Service.Querier.Instance(),
				a.Service.NotifyingMutator.Instance(),
			))
		})
	})
}
