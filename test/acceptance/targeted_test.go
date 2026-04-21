package acceptance_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"gotest.tools/v3/assert"
)

func TestShapeTargeted(t *testing.T) {
	err := withFixture(
		func(ctx FixtureContext) error {
			err := shape.Run(&shape.Config{
				Macros: []config.MacroConfig{
					{
						PlumberMacro: &config.PlumberMacroConfig{
							Name: "@derive",
							Annotations: []config.AnnotationConfig{
								{Name: "plumber:derive", Args: []string{`{{ index .Macro.Args 0 }}`}},
								{Name: "plumber:output", Args: []string{"generated.go"}},
							},
						},
					},
				},
				Target: &config.TargetConfig{
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
				Target: &config.TargetConfig{
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
				Macros: []config.MacroConfig{
					{
						PlumberMacro: &config.PlumberMacroConfig{
							Name: "@derive",
							Annotations: []config.AnnotationConfig{
								{Name: "plumber:derive", Args: []string{"Derived"}},
								{Name: "plumber:output", Args: []string{"generated.go"}},
							},
						},
					},
				},
				Target: &config.TargetConfig{
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
