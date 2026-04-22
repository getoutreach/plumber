package shape

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/query/model"
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
				Filename: "file.go",
			},
			expected: "generated.go",
		},
		{
			name: "output with template name and ext",
			position: model.Position{
				Filename: "example.go",
			},
			expected: "example_generated.go",
			annotations: []model.Annotation{
				model.NewAnnotation(contract.OptionOutput, []string{`{{ .Name }}_generated{{ .Ext }}`}),
			},
		},
		{
			name: "output with suffixed function",
			position: model.Position{
				Filename: "example.go",
			},
			expected: "example_generated.go",
			annotations: []model.Annotation{
				model.NewAnnotation(contract.OptionOutput, []string{`{{ suffixed "generated" }}`}),
			},
		},
		{
			name: "output with filename template",
			position: model.Position{
				Filename: "example.go",
			},
			expected: "example.go.bak",
			annotations: []model.Annotation{
				model.NewAnnotation(contract.OptionOutput, []string{`{{ .Filename }}.bak`}),
			},
		},
		{
			name: "inplace mode honors output annotation",
			position: model.Position{
				Filename: "example.go",
			},
			expected: "merged.go",
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
			expected: "generated.go",
			annotations: []model.Annotation{
				model.NewAnnotation(contract.OptionMode, []string{"inplace"}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := &BasicTransformer{
				Position:    tt.position,
				Annotations: tt.annotations,
			}
			output := transformer.Output()
			if output != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, output)
			}
		})
	}
}
