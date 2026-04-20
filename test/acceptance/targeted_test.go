package acceptance_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"gotest.tools/v3/assert"
)

func TestShapeTargeted(t *testing.T) {
	err := withFixture(
		func(ctx FixtureContext) error {
			err := shape.Run(&shape.Config{
				Macros: []shape.MacroConfig{
					{
						PlumberMacro: &shape.PlumberMacroConfig{
							Name: "@derive",
							Annotations: []shape.AnnotationConfig{
								{Name: "plumber:derive", Args: []string{`{{ index .Args 0 }}`}},
								{Name: "plumber:output", Args: []string{"generated.go"}},
							},
						},
					},
				},
				Target: &shape.TargetConfig{
					TypeFQN: "Worker",
					Macro:   "@derive",
					Args:    []string{"WorkerDerived"},
				},
			}, []string{"./..."})
			assert.NilError(t, err)
			ctx.AssertContent(t, "targeted/generated.go", "targeted/generated.go.golden")
			return nil
		},
		"targeted/model.go",
	)
	assert.NilError(t, err)
}

func TestShapeTargetedMacroNotFound(t *testing.T) {
	err := withFixture(
		func(_ FixtureContext) error {
			return shape.Run(&shape.Config{
				Target: &shape.TargetConfig{
					TypeFQN: "Worker",
					Macro:   "@nonexistent",
					Args:    []string{"Derived"},
				},
			}, []string{"./..."})
		},
		"targeted/model.go",
	)
	assert.ErrorContains(t, err, `macro "@nonexistent" not found in config`)
}

func TestShapeTargetedTypeNotFound(t *testing.T) {
	err := withFixture(
		func(_ FixtureContext) error {
			return shape.Run(&shape.Config{
				Macros: []shape.MacroConfig{
					{
						PlumberMacro: &shape.PlumberMacroConfig{
							Name: "@derive",
							Annotations: []shape.AnnotationConfig{
								{Name: "plumber:derive", Args: []string{"Derived"}},
								{Name: "plumber:output", Args: []string{"generated.go"}},
							},
						},
					},
				},
				Target: &shape.TargetConfig{
					TypeFQN: "NonExistent",
					Macro:   "@derive",
					Args:    []string{"Derived"},
				},
			}, []string{"./..."})
		},
		"targeted/model.go",
	)
	assert.ErrorContains(t, err, "not found in inspected packages")
}
