// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: graphql infra for example application

// Package graphql provides graphql infra for example application
package graphql

import (
	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/contract"
	"github.com/getoutreach/plumber/example/service"
)

// Server represents a graphql server
type Server struct {
	*plumber.BaseLooper
	port    int32
	querier *service.QueryService
	mutator contract.MutatorService
}

// NewServer returns intance of the *Server
func NewServer(
	port int32,
	querier *service.QueryService,
	mutator contract.MutatorService,
) (*Server, error) {
	return &Server{
		port:       port,
		querier:    querier,
		mutator:    mutator,
		BaseLooper: contract.NewWorker("graphql.Server"),
	}, nil
}
