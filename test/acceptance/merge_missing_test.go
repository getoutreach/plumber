package acceptance_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"gotest.tools/v3/assert"
)

// TestMergeMissingType verifies that when an inplace derive transformation targets a type
// that does not yet exist in the package, the generated declaration is appended to the
// inplace output file (named according to the transformer's Output()), creating the file
// if necessary, instead of failing with a "type not found" error.
func TestMergeMissingType(t *testing.T) {
	err := withFixture(
		&shape.Config{},
		func(ctx FixtureContext) error {
			err := shape.Run(ctx.ShapingContext, ctx.Cfg, []string{"./..."})
			assert.NilError(t, err)
			ctx.AssertContent(t, "mergemissing/merged.go", "mergemissing/merged.go.golden")
			return nil
		},
		"mergemissing/model.go",
		"mergemissing/types.go",
	)
	assert.NilError(t, err)
}
