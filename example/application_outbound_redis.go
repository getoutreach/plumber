package example

import (
	"context"
	// <<plumber::Block(imports)>>
	// <</plumber::Block>>

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/discovery"
	"github.com/getoutreach/plumber/example/adapter/outbound/redis"
)

// <<plumber::Block(header)>>
// <</plumber::Block>>

// OutboundRedis dependency container
type OutboundRedis struct {
	Dep1      plumber.D[*redis.Dep1]
	Dep1Named plumber.D[*redis.Dep1]
	Client    plumber.D[*redis.Client]
}

// Define dependency resolvers
func (c *OutboundRedis) Define(ctx context.Context, cf *Config, a *Container) {
	c.Dep1.Resolver(func(r *plumber.Resolution[*redis.Dep1]) {
		r.Require().Then(func() {
			r.ResolveError(redis.NewDep1())
		})
	})
	c.Dep1Named.Resolver(func(r *plumber.Resolution[*redis.Dep1]) {
		r.Require().Then(func() {
			r.ResolveError(redis.NewDep1Named())
		})
	})

	c.Client.Resolver(func(r *plumber.Resolution[*redis.Client]) {
		r.Require(
			&c.Dep1Named,
			&c.Dep1,
		).Then(func() {
			r.ResolveError(redis.NewClient(
				discovery.OneOf(
					c.Dep1.Instance(),
					c.Dep1Named.Instance(),
				),
			))
		})
	})
}

// <<plumber::Block(footer)>>
// <</plumber::Block>>
