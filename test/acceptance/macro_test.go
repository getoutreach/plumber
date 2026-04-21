package acceptance_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"gotest.tools/v3/assert"
)

func TestMacro(t *testing.T) {
	err := withFixture(
		func(ctx FixtureContext) error {
			err := shape.Run(&shape.Config{
				Macros: []config.MacroConfig{
					{
						PlumberMacro: &config.PlumberMacroConfig{
							Name: "@derive",
							Annotations: []config.AnnotationConfig{
								{Name: "plumber:derive", Args: []string{"{name}Macro"}},
								{Name: "plumber:output", Args: []string{"generated.go"}},
							},
						},
					},
				},
			}, []string{"./..."})
			assert.NilError(t, err)
			ctx.AssertContent(t, "macro/generated.go", "macro/generated.go.golden")
			return nil
		},
		"macro/model.go",
	)
	assert.NilError(t, err)
}

func TestMacroTemplate(t *testing.T) {
	err := withFixture(
		func(ctx FixtureContext) error {
			err := shape.Run(&shape.Config{
				Macros: []config.MacroConfig{
					{
						PlumberMacro: &config.PlumberMacroConfig{
							Name: "@tderive",
							Annotations: []config.AnnotationConfig{
								{Name: "plumber:derive", Args: []string{`{{ index .Macro.Args 0 }}`}},
								{Name: "plumber:output", Args: []string{"generated.go"}},
							},
						},
					},
				},
			}, []string{"./..."})
			assert.NilError(t, err)
			ctx.AssertContent(t, "macrotemplate/generated.go", "macrotemplate/generated.go.golden")
			return nil
		},
		"macrotemplate/model.go",
	)
	assert.NilError(t, err)
}
