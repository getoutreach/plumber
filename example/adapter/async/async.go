// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: async infra for example application

// Package async provides async infra for example application
package async

import (
	"context"
	"fmt"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example/contract"
)

// Publisher service
type Publisher struct {
	*plumber.BaseLooper
	broker string
}

func NewPublisher(broker string) *Publisher {
	return &Publisher{
		broker:     broker,
		BaseLooper: contract.NewWorker("async.Publisher"),
	}
}

func (p *Publisher) Publish(ctx context.Context, e *contract.Entity) error {
	fmt.Printf("Publishing entity #%v name=%s\n", e.ID, e.Name)
	return nil
}
