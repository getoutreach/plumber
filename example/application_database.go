// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: database related dependencies
package example

import (
	"context"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/adapter/database"
	"github.com/getoutreach/plumber/example/contract"
)

// Database represents database related dependency container
type Database struct {
	Repository         plumber.D[contract.Repository]
	BatchingRepository plumber.R[*database.BatchingRepository]
	UserRepository     plumber.D[*database.UserRepository]
}

// Define resolves dependencies
func (c *Database) Define(ctx context.Context, cf *Config, a *Container) {
	c.Repository.DefineError(func() (contract.Repository, error) {
		return database.NewRepository()
	})

	c.BatchingRepository.Resolver(func(r *plumber.ResolutionR[*database.BatchingRepository]) {
		r.Require(
			&c.Repository,
		).Then(func() {
			r.ResolveError(database.NewBatchingRepository(c.Repository.Instance(), 100))
		})
	})
	c.UserRepository.Resolver(func(r *plumber.Resolution[*database.UserRepository]) {
		r.Require().Then(func() {
			r.ResolveError(database.NewUserRepository())
		})
	}) //1111
}
