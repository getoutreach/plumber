package shape

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	baserender "github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
	"gotest.tools/v3/assert"
)

func TestOutput(t *testing.T) {
	tests := []struct {
		name        string
		position    model.Position
		expected    string
		annotations []model.Annotation
	}{
		{
			name: "simple output",
			position: model.Position{
				Filename: "pkg/file.go",
			},
			expected: "pkg/generated.go",
		},
		{
			name: "output with template name and ext",
			position: model.Position{
				Filename: "example.go",
			},
			expected: "pkg/example_generated.go",
			annotations: []model.Annotation{
				model.NewAnnotation(contract.OptionOutput, []string{`{{ .Output.Name }}_generated{{ .Output.Ext }}`}),
			},
		},
		{
			name: "output with suffixed function",
			position: model.Position{
				Filename: "example.go",
			},
			expected: "pkg/example_generated.go",
			annotations: []model.Annotation{
				model.NewAnnotation(contract.OptionOutput, []string{`{{ filename_suffixed "generated" }}`}),
			},
		},
		{
			name: "output with filename template",
			position: model.Position{
				Filename: "example.go",
			},
			expected: "pkg/example.go.bak",
			annotations: []model.Annotation{
				model.NewAnnotation(contract.OptionOutput, []string{`{{ .Output.Filename }}.bak`}),
			},
		},
		{
			name: "inplace mode honors output annotation",
			position: model.Position{
				Filename: "example.go",
			},
			expected: "pkg/merged.go",
			annotations: []model.Annotation{
				model.NewAnnotation(contract.OptionMode, []string{"inplace"}),
				model.NewAnnotation(contract.OptionOutput, []string{"merged.go"}),
			},
		},
		{
			name: "inplace mode falls back to default output",
			position: model.Position{
				Filename: "example.go",
			},
			expected: "pkg/generated.go",
			annotations: []model.Annotation{
				model.NewAnnotation(contract.OptionMode, []string{"inplace"}),
			},
		},
	}

	pkg := &model.Package{Dir: "pkg", Path: "github.com/example/repo/pkg"}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := &BasicTransformer{
				Package:     pkg,
				Position:    tt.position,
				Annotations: tt.annotations,
			}
			transformer.Validate(pkg) // Ensure annotations are processed before generating output
			err := transformer.Expand(&contract.ShapingContext{
				RepoModule: contract.ModuleInfo{
					// Name: "repo",
					// Path: "github.com/example/repo",
					// Dir:  "/",
				},
				Module: contract.ModuleInfo{
					Name: "pkg",
					Path: "github.com/example/repo/pkg",
					Dir:  "pkg",
				},
			}, []*model.Package{pkg}, &model.Type{
				TypeNode: &model.TypeNode{
					Package:  pkg,
					Position: tt.position,
				},
			}, baserender.Scope{}, nil)
			assert.NilError(t, err)
			output := transformer.Output()
			if output != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, output)
			}
		})
	}
}
