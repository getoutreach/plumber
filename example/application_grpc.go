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
	Port          plumber.D[int32]
	Server        plumber.R[*grpc.Server]
	TracingServer plumber.R[*grpc.Server]
	WithTracing   plumber.D[func(*grpc.Server) *grpc.Server]
}

// Define resolves dependencies
func (c *GRPC) Define(ctx context.Context, cf *Config, a *Container) {
	c.Port.Const(5000)

	c.Server.Resolver(func(r *plumber.ResolutionR[*grpc.Server]) {
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

	c.WithTracing.Const(func(s *grpc.Server) *grpc.Server {
		// wrap with tracing interceptor
		return s
	})

	c.TracingServer.As(&c.Server).Wrap(
		&c.WithTracing,
		plumber.WrapperFunc(func(s *grpc.Server) *grpc.Server { return s }),
	)
}
