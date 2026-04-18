
// Copyright 2026 Outreach Corporation. All Rights Reserved.
// Description: OutboundRedis related dependencies example
package example

import (
  "context"
)

// OutboundRedis dependency container
type OutboundRedis struct {}

// Define dependency resolvers
func (c *OutboundRedis) Define(ctx context.Context, cf *Config, a *Container) {
}
