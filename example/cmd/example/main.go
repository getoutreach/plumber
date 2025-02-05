// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: example application

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example"
)

func main() {
	a := example.NewApplication(context.Background(), &example.Config{
		AsyncBroker: "broker.service:9092",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := plumber.Start(ctx,
		// First we start service in sequence to ensure that publisher run fist
		plumber.Pipeline(
			&a.Database.BatchingRepository,
			&a.Async.Publisher,
			// All good so we can start other services in parallel
			plumber.Parallel(
				&a.GRPC.Server,
				&a.GraphQL.Server,
			),
		),
		// The pipeline will run for 10 seconds then will be closed gracefully.
		plumber.TTL(5*time.Second),
		// The pipeline needs to finish startup phase within 1 seconds. If not, run context is canceled. Close is initiated.
		plumber.ReadinessTimeout(1*time.Second),
		// The pipeline needs to gracefully close with 5 seconds. If not, internal run and close contexts are canceled.
		plumber.CloseTimeout(5*time.Second),
		// The pipeline will stop as well on OS signal
		plumber.SignalCloser(),
	)
	if err != nil {
		fmt.Println("err: ", err)
	}
}
