// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines the Scenario struct and constructor for composing example application components.

// Package scenario provides an example scenario that demonstrates how to compose plumber-managed components.
package scenario

import "github.com/getoutreach/plumber/example/adapter/async"

type Scenario struct {
	Publisher *async.Publisher
}

func NewScenario(publisher *async.Publisher) *Scenario {
	return &Scenario{}
}
