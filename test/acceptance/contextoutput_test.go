package acceptance_test

import (
	"os"
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/template"
	"gotest.tools/v3/assert"
)

// describeTemplate renders a new method on the shaped (context) type.
const describeTemplate = `{{define "plumber/command/shape/struct"}}
// Describe returns the worker name.
func (w *{{ $.Type.Name }}) Describe() string {
	return w.Name
}
{{end}}`

// TestContextInplaceOutput verifies that in inplace mode an explicit plumber:output is
// preferred over the file of the type located by plumber:context: new methods on the
// context type are written to the output file and the context type's file is left
// untouched.
func TestContextInplaceOutput(t *testing.T) {
	cfg := &shape.Config{}
	cfg.Templates.Content = []template.ContentConfig{{Name: "worker.describe", Content: describeTemplate}}
	err := withFixture(
		cfg,
		func(ctx FixtureContext) error {
			original, err := os.ReadFile("contextoutput/worker.go")
			assert.NilError(t, err)

			err = shape.Run(ctx.ShapingContext, ctx.Cfg, nil)
			assert.NilError(t, err)

			ctx.AssertContent(t, "contextoutput/worker_describe.go", "contextoutput/worker_describe.go.golden")
			after, err := os.ReadFile("contextoutput/worker.go")
			assert.NilError(t, err)
			assert.Equal(t, string(after), string(original))
			return nil
		},
		"contextoutput/worker.go",
		"contextoutput/accessor.go",
	)
	assert.NilError(t, err)
}
