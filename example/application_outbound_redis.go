// Copyright 2025 Outreach Corporation. All Rights Reserved.
// Description: OutboundRedis related dependencies example
package example

import (
	"context"
	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/adapter/outbound/redis"
)

// OutboundRedis dependency container
type OutboundRedis struct {
	Client plumber.D[*redis.Client]
}

// Define dependency resolvers
func (c *OutboundRedis) Define(ctx context.Context, cf *Config, a *Container) {
	c.Client.Resolver(func(r *plumber.Resolution[*redis.Client]) {
		r.Require().Then(func() {
			r.ResolveError(redis.NewClient())
		})
	})
}
