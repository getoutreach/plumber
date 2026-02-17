package // Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: database related dependencies
// Database represents database related dependency container
// Define resolves dependencies
example

import (
	"context"
	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/adapter/database"
	"github.com/getoutreach/plumber/example/contract"
)

type Database struct {
	Repository         plumber.D[contract.Repository]
	BatchingRepository plumber.R[*database.BatchingRepository]
	UserRepository     plumber.R[*database.UserRepository]
}

func (c *Database) Define(ctx context.Context, cf *Config, a *Container) {
	c.Repository.DefineError(func() (contract.Repository, error) {
		return database.NewRepository()
	})
	c.BatchingRepository.Resolver(func(r *plumber.ResolutionR[*database.BatchingRepository]) {
		r.Require(&c.Repository).Then(func() {
			r.ResolveError(database.NewBatchingRepository(c.Repository.Instance(), 100))
		})
	})
}
