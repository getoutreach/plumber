package acceptance_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"gotest.tools/v3/assert"
)

func TestQuery(t *testing.T) {
	err := withFixture(
		&shape.Config{},
		func(ctx FixtureContext) error {
			err := shape.Run(ctx.ShapingContext, ctx.Cfg, nil)
			assert.NilError(t, err)
			ctx.AssertContent(t, "query/consumer.go", "query/consumer.go.golden")
			return nil
		},
		"query/providers.go",
		"query/consumer.go",
	)
	assert.NilError(t, err)
}

func TestQueryTypeScope(t *testing.T) {
	err := withFixture(
		&shape.Config{},
		func(ctx FixtureContext) error {
			err := shape.Run(ctx.ShapingContext, ctx.Cfg, nil)
			assert.NilError(t, err)
			ctx.AssertContent(t, "querytypescope/consumer.go", "querytypescope/consumer.go.golden")
			return nil
		},
		"querytypescope/types.go",
		"querytypescope/consumer.go",
	)
	assert.NilError(t, err)
}

func TestQueryCrossPackage(t *testing.T) {
	err := withFixture(
		&shape.Config{},
		func(ctx FixtureContext) error {
			err := shape.Run(ctx.ShapingContext, ctx.Cfg, nil)
			assert.NilError(t, err)
			ctx.AssertContent(t, "querycross/consumer.go", "querycross/consumer.go.golden")
			return nil
		},
		"querycross/providers/providers.go",
		"querycross/consumer.go",
	)
	assert.NilError(t, err)
}

func TestQueryLocal(t *testing.T) {
	err := withFixture(
		&shape.Config{},
		func(ctx FixtureContext) error {
			err := shape.Run(ctx.ShapingContext, ctx.Cfg, nil)
			assert.NilError(t, err)
			ctx.AssertContent(t, "querylocal/consumer.go", "querylocal/consumer.go.golden")
			return nil
		},
		"querylocal/providers.go",
		"querylocal/consumer.go",
	)
	assert.NilError(t, err)
}
