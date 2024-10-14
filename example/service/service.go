// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: services for example application

// Package service provides services for example application
package service

import (
	"context"
	"fmt"

	"github.com/getoutreach/plumber/example/adapter/async"
	"github.com/getoutreach/plumber/example/contract"
)

// QueryService is a service that serve entity read
type QueryService struct {
	repository contract.Repository
}

func NewQueryService(repository contract.Repository) *QueryService {
	return &QueryService{
		repository: repository,
	}
}

func (s *QueryService) Get(ctx context.Context, id int64) (*contract.Entity, error) {
	return s.repository.Get(ctx, id)
}

// MutatorService is a service that facilitates entity creation
type MutatorService struct {
	repository contract.Repository
}

func NewMutatorService(repository contract.Repository) *MutatorService {
	return &MutatorService{
		repository: repository,
	}
}

func (s *MutatorService) Create(ctx context.Context, name string) (*contract.Entity, error) {
	return s.repository.Create(ctx, name)
}

// NotifyingMutatorService is a service that notifies publisher during entity creation
type NotifyingMutatorService struct {
	inner     contract.MutatorService
	publisher *async.Publisher
}

func NewNotifyingMutatorService(inner contract.MutatorService) *NotifyingMutatorService {
	return &NotifyingMutatorService{
		inner: inner,
	}
}

func (s *NotifyingMutatorService) Create(ctx context.Context, name string) (*contract.Entity, error) {
	e, err := s.inner.Create(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("can't create an entity:%w", err)
	}
	return e, s.publisher.Publish(ctx, e)
}
