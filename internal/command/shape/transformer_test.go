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
			name: "output with suffix",
			position: model.Position{
				Filename: "example.go",
			},
			expected: "example_generated.go",
			annotations: []model.Annotation{
				model.NewAnnotation(contract.OptionOutput, []string{"{name}_generated{ext}"}),
			},
		},
		{
			name: "output with suffixed output",
			position: model.Position{
				Filename: "example.go",
			},
			expected: "example_generated.go",
			annotations: []model.Annotation{
				model.NewAnnotation(contract.OptionOutput, []string{"{suffix:generated}"}),
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
