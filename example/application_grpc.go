// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: grpc related dependencies
package example

import (
	"context"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/adapter/grpc"
)

// GRPC service represents grpc related dependency container
type GRPC struct {
	Port   plumber.D[int32]
	Server plumber.R[*grpc.Server]
}

// Define resolves dependencies
func (c *GRPC) Define(ctx context.Context, cf *Config, a *Container) {
	c.Port.Const(5000)

	c.Server.Resolve(func(r *plumber.ResolutionR[*grpc.Server]) {
		r.Require(
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
