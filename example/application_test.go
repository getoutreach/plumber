package example_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/getoutreach/plumber"
	"github.com/getoutreach/plumber/example"
	"gotest.tools/v3/assert"
)

// WithIntegrationEnvironment redefines application
// graph for integration environment
func WithIntegrationEnvironment(
	ctx context.Context, cf *example.Config, a *example.Container,
) {
	a.GRPC.Port.Const(1000)
}

func TestApplicationManually(t *testing.T) {
	cfg := &example.Config{}
	a := example.NewApplication(
		context.Background(), cfg, WithIntegrationEnvironment,
	)

	// You can manually assert specific instances
	assert.Equal(t, int32(1000), a.GRPC.Port.Instance())
	assert.Assert(t, plumber.Resolved(
		&a.Database.BatchingRepository,
		&a.Async.Publisher,
		&a.GRPC.Server,
		&a.GraphQL.Server,
	))
}

func TestPtr(t *testing.T) {
	cfg := &example.Config{}
	c := example.NewApplication(
		context.Background(), cfg, WithIntegrationEnvironment,
	)

	v := plumber.ReflectValueByPath(func() *example.Container { return c }, []string{"Async", "Publisher"})

	if vp, ok := v.Addr().Interface().(plumber.Errorer); ok {
		fmt.Println(vp.Error())
	}
}

func TestApplicationContainer(t *testing.T) {
	// Or you can and you should check the whole container
	if err := plumber.ContainerResolved(func() *example.Container {
		cfg := &example.Config{}
		c := example.NewApplication(
			context.Background(), cfg, WithIntegrationEnvironment,
		)
		return c
	}); err != nil {
		assert.Assert(t, err != nil)
		assert.Equal(t, err.Error(),
			// nolint: lll //Why: multiline string
			`errors on "Bugs.Notdefined": instance notdefined(*async.Publisher) not resolved
errors on "Bugs.GraphQL": dependency not resolved, *graphql.Server requires notdefined(*async.Publisher) (instance notdefined(*async.Publisher) not resolved)
errors on "Bugs.GraphQL": unused dependency: notdefined(*async.Publisher)`,
		)
	}
}
