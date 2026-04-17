package acceptance_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"gotest.tools/v3/assert"
)

func TestMerge(t *testing.T) {
	err := withFixture(
		func(ctx FixtureContext) error {
			err := shape.Run(&shape.Config{}, []string{"./..."})
			assert.NilError(t, err)
			ctx.AssertContent(t, "merge/blended.go", "merge/blended.go.golden")
			return nil
		},
		"merge/blended.go",
		"merge/model.go",
		"merge/types.go",
	)
	assert.NilError(t, err)
}
