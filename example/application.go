// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: application root dependency container
package example

import (
	"context"

	"github.com/getoutreach/plumber"
)

// Config represents a application configuration structure
type Config struct {
	AsyncBroker string
}

// Definer allows to redefine container on startup
type Definer = func(ctx context.Context, cf *Config, a *Container)

// Container represents root application dependency container
type Container struct {
	plumber.Container
	Async    *Async
	Database *Database
	GraphQL  *GraphQL
	GRPC     *GRPC
	Service  *Service
}

// NewApplication returns instance of the root dependency container
func NewApplication(ctx context.Context, cf *Config, definers ...Definer) *Container {
	a := &Container{
		GRPC:     new(GRPC),
		Database: new(Database),
		GraphQL:  new(GraphQL),
		Service:  new(Service),
		Async:    new(Async),
	}
	return plumber.DefineContainers(ctx, cf, definers, a,
		a.Async, a.Database, a.GRPC, a.GraphQL, a.Service,
	)
}
