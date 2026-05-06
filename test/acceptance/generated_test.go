package acceptance_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"gotest.tools/v3/assert"
)

func TestGenerated(t *testing.T) {
	err := withFixture(
		&shape.Config{
			Mixins: []config.MixinConfig{
				{
					PlumberMixin: &config.PlumberMixinConfig{
						Name: "mixing.model.filtrable",
						Annotations: []config.AnnotationConfig{
							{
								Name: "plumber:filter",
								Args: []string{"annotation.has", "is:filtrable"},
							},
						},
					},
				},
			},
		},
		func(ctx FixtureContext) error {
			err := shape.Run(ctx.ShapingContext, ctx.Cfg, []string{"./..."})
			assert.NilError(t, err)
			ctx.AssertContent(t, "generated/generated.go", "generated/generated.go.golden")
			return nil
		},
		"generated/model.go",
		"generated/types.go",
	)
	assert.NilError(t, err)
}
