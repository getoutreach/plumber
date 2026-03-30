package render

import (
	"fmt"
	"testing"

	"github.com/getoutreach/plumber/internal/render/view"
	"github.com/getoutreach/plumber/query/model"
)

func TestTesting(t *testing.T) {

	context := Context{
		Modules: NewModuleRegister(),
		Ignores: NewIgnores([]string{"ID"}),
	}

	tp := &model.Type{
		Name: "MyStruct",
		Spec: model.TypeSpec{
			FQN: "github.com/getoutreach/plumber/internal/render/render_test.MyStruct",
		},
		TypeNode: &model.TypeNode{
			Annotations: []model.Annotation{
				{
					Name: "plumber:comment",
					Args: []string{"My struct comment"},
				},
				{
					Name: "plumber:name",
					Args: []string{"MyBetterStruct"},
				},
			},
		},
		Struct: &model.Struct{
			Fields: []*model.Var{
				{
					Name: "ID",
					Type: &model.TypeDefinition{
						Spec: model.TypeSpec{
							FQN: "string",
						},
					},
				},
				{
					Name: "Name",
					Type: &model.TypeDefinition{
						Spec: model.TypeSpec{
							FQN: "string",
						},
					},
					Annotations: []model.Annotation{
						{
							Name: "plumber:comment",
							Args: []string{"My comment"},
						},
					},
				},
				{
					Name: "Model",
					Type: &model.TypeDefinition{
						Spec: model.TypeSpec{
							FQN: `*"github.com/getoutreach/plumber/query/model".Type`,
						},
					},
				},
			},
		},
	}

	content, err := Derive(context, tp, map[string]any{
		"Derive": view.Annotable{
			Annotations: []model.Annotation{
				model.NewAnnotation("plumber:name", "MyDerivedStruct"),
				model.NewAnnotation("plumber:comment", "Better struct comment"),
			},
		},
	}, "derived_output.go")
	if err != nil {
		fmt.Println("Error during rendering:", err)
	}

	fmt.Printf("Done: %v\n", string(content))
}
