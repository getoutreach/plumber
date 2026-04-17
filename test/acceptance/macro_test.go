package acceptance_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"gotest.tools/v3/assert"
)

func TestMacro(t *testing.T) {
	err := withFixture(
		func(ctx FixtureContext) error {
			err := shape.Run(&shape.Config{
				Macros: []shape.MacroConfig{
					{
						PlumberMacro: &shape.PlumberMacroConfig{
							Name: "@derive",
							Annotations: []shape.AnnotationConfig{
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
				Macros: []shape.MacroConfig{
					{
						PlumberMacro: &shape.PlumberMacroConfig{
							Name: "@tderive",
							Annotations: []shape.AnnotationConfig{
								{Name: "plumber:derive", Args: []string{`{{ index .Args 0 }}`}},
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
