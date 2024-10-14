package example_test

import (
	"context"
	"testing"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example"
	"gotest.tools/v3/assert"
)

func TestApplication(t *testing.T) {
	cfg := &example.Config{}
	a := example.NewApplication(context.Background(), cfg, example.WithIntegrationEnvironment)

	assert.Equal(t, int32(1000), a.GRPC.Port.Instance())

	assert.Assert(t, plumber.Resolved(
		&a.Database.BatchingRepository,
		&a.Async.Publisher,
		&a.GRPC.Server,
		&a.GraphQL.Server,
	))
}
