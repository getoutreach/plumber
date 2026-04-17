package acceptance_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"gotest.tools/v3/assert"
)

func TestGenerated(t *testing.T) {
	err := withFixture(
		func(ctx FixtureContext) error {
			err := shape.Run(&shape.Config{
				Mixins: []shape.MixinConfig{
					{
						PlumberMixin: &shape.PlumberMixinConfig{
							Name: "mixing.model.filtrable",
							Annotations: []shape.AnnotationConfig{
								{
									Name: "plumber:filter",
									Args: []string{"annotation.has", "is:filtrable"},
								},
							},
						},
					},
				},
			}, []string{"./..."})
			assert.NilError(t, err)
			ctx.AssertContent(t, "generated/generated.go", "generated/generated.go.golden")
			return nil
		},
		"generated/model.go",
		"generated/types.go",
	)
	assert.NilError(t, err)
}
