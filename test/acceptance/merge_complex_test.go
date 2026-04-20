package acceptance_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/template"
	"gotest.tools/v3/assert"
)

func TestMergeComplex(t *testing.T) {
	// Content template that overrides plumber/command/derive to produce
	// a struct + constructor + method, exercising the full merge pipeline:
	// - struct field merge (add missing fields)
	// - function param merge (add missing params)
	// - body merge with non-empty existing (subsequence matching)
	// - deep merge of composite literal (add missing key-value entries)
	// - deep merge of call arguments (add missing args)
	//
	// Note: We cannot use custom template functions (type, expand_name, lower, etc.)
	// in content templates because they are parsed before the funcmap is available.
	// Instead we hardcode the output using only built-in template syntax.
	contentTemplate := `{{define "plumber/command/derive"}}
type ServiceBlended struct {
	Logger Logger
	DB     Database
	Cache  Cache
}

func NewServiceBlended(logger Logger, db Database, cache Cache) *ServiceBlended {
	return &ServiceBlended{
		Logger: logger,
		DB:     db,
		Cache:  cache,
	}
}

func (s *ServiceBlended) Start() error {
	s.Logger.Info("starting", "service")
	_ = s.DB.Ping()
	return nil
}

func (s *ServiceBlended) Switch(str string) {
    switch str {
    case "case1":
        s.Logger.Info("switching to case1")
    case "case2":
        s.Logger.Info("switching to case2")
    default:
        s.Logger.Info("switching to default case")
    }
}
{{end}}`

	err := withFixture(
		func(ctx FixtureContext) error {
			err := shape.Run(&shape.Config{
				Templates: template.TemplatesFileConfig{
					Content: []template.ContentConfig{
						{
							Name:    "mergecomplex-override",
							Content: contentTemplate,
						},
					},
				},
			}, []string{"./..."})
			assert.NilError(t, err)
			ctx.AssertContent(t, "mergecomplex/blended.go", "mergecomplex/blended.go.golden")
			return nil
		},
		"mergecomplex/blended.go",
		"mergecomplex/model.go",
		"mergecomplex/types.go",
	)
	assert.NilError(t, err)
}
