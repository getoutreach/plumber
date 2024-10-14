// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: env specific overrides
package example

import "context"

// WithTestEnvironment redefines application dependency graph for test environment
func WithTestEnvironment(ctx context.Context, cf *Config, a *Container) {
	a.GRPC.Port.Const(1001)
}

// WithIntegrationEnvironment redefines application graph for integration environment
func WithIntegrationEnvironment(ctx context.Context, cf *Config, a *Container) {
	a.GRPC.Port.Const(1000)
}
