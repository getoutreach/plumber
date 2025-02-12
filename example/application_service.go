// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: application service dependencies

package example

import (
	"context"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/contract"
	"github.com/getoutreach/plumber/example/service"
)

// Service represents database related dependency container
type Service struct {
	Mutator          plumber.D[contract.MutatorService]
	NotifyingMutator plumber.D[contract.MutatorService]
	Querier          plumber.D[*service.QueryService]
}

// Define resolves dependencies
func (c *Service) Define(ctx context.Context, cf *Config, a *Container) {
	c.Mutator.Resolver(func(r *plumber.Resolution[contract.MutatorService]) {
		r.Require(
			&a.Database.Repository,
		).Then(func() {
			r.Resolve(service.NewMutatorService(a.Database.Repository.Instance()))
		})
	})

	c.NotifyingMutator.Resolver(func(r *plumber.Resolution[contract.MutatorService]) {
		r.Require(
			&c.Mutator,
		).Then(func() {
			r.Resolve(service.NewNotifyingMutatorService(c.Mutator.Instance()))
		})
	})

	c.Querier.Resolver(func(r *plumber.Resolution[*service.QueryService]) {
		r.Require(
			&a.Database.Repository,
		).Then(func() {
			r.Resolve(service.NewQueryService(a.Database.BatchingRepository.Instance()))
		})
	})
}
