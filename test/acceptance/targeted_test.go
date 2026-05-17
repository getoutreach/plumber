package acceptance_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"gotest.tools/v3/assert"
)

func TestShapeTargeted(t *testing.T) {
	t.Skip("Skipping until package is correctly resolved in target mode")
	err := withFixture(
		&shape.Config{
			Macros: []config.MacroConfig{
				{
					PlumberMacro: &config.PlumberMacroConfig{
						Name: "@derive",
						Annotations: []config.AnnotationConfig{
							{Name: "plumber:derive", Args: []string{`{{ index .Source.Args 0 }}`}},
							{Name: "plumber:output", Args: []string{"targeted/generated.go"}},
						},
					},
				},
			},
			Target: &config.TargetConfig{
				TypeFQN: "Worker",
				Macro:   "@derive",
				Args:    []string{"WorkerDerived"},
			},
		},
		func(ctx FixtureContext) error {
			err := shape.RunTarget(ctx.ShapingContext, ctx.Cfg, nil)
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
		&shape.Config{
			Target: &config.TargetConfig{
				TypeFQN: "Worker",
				Macro:   "@nonexistent",
				Args:    []string{"Derived"},
			},
		},
		func(ctx FixtureContext) error {
			return shape.RunTarget(ctx.ShapingContext, ctx.Cfg, nil)
		},
		"targeted/model.go",
	)
	assert.ErrorContains(t, err, `macro "@nonexistent" not found in config`)
}

func TestShapeTargetedTypeNotFound(t *testing.T) {
	err := withFixture(
		&shape.Config{
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
		},
		func(ctx FixtureContext) error {
			return shape.RunTarget(ctx.ShapingContext, ctx.Cfg, nil)
		},
		"targeted/model.go",
	)
	assert.ErrorContains(t, err, "not found in inspected packages")
}
