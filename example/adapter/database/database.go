// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: database infra for example application

// Package database provides database infra for example application
package database

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/contract"
)

// EntityMask provides a mask to format an entity
const EntityMask = "Entity %v"

// Repository represents a plain database repository
type Repository struct {
	id int64
}

func NewRepository() (*Repository, error) {
	return &Repository{}, nil
}

func (s *Repository) Get(ctx context.Context, id int64) (*contract.Entity, error) {
	return &contract.Entity{
		ID:   id,
		Name: fmt.Sprintf(EntityMask, id),
	}, nil
}

func (s *Repository) Create(ctx context.Context, name string) (*contract.Entity, error) {
	nextID := atomic.AddInt64(&s.id, 1)
	return &contract.Entity{
		ID:   nextID,
		Name: fmt.Sprintf(EntityMask, nextID),
	}, nil
}

// BatchingRepository represents a database repository that can batch single entity reads into batch query
type BatchingRepository struct {
	*plumber.BaseLooper
	inner contract.Repository
}

func NewBatchingRepository(inner contract.Repository, batchSize int) (*BatchingRepository, error) {
	r := &BatchingRepository{
		inner:      inner,
		BaseLooper: contract.NewWorker("database.BatchingRepository"),
	}
	return r, nil
}

func (s *BatchingRepository) Get(ctx context.Context, id int64) (*contract.Entity, error) {
	// Here suppose to be a logic that batches requests into a single batch query
	return s.inner.Get(ctx, id)
}

func (s *BatchingRepository) Create(ctx context.Context, name string) (*contract.Entity, error) {
	return s.inner.Create(ctx, name)
}
