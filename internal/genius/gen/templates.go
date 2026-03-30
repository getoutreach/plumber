package gen

import (
	"embed"
	"strings"
	"text/template"

	"github.com/pkg/errors"

	"github.com/gertd/go-pluralize"
	conv "github.com/getoutreach/plumber/internal/genius/format"
)

const (
	GenericTemplatesDir = "templates"
)

var (
	plr = pluralize.NewClient()

	//go:embed templates/*
	BaseTemplates embed.FS

	BaseFuncMap = template.FuncMap{
		"upper":     strings.ToUpper,
		"lower":     strings.ToLower,
		"dict":      createDict,
		"pluralize": pluralizeString(plr),
		"pascal":    conv.PascalCase,
		"camel":     conv.CamelCase,
		"snake":     conv.SnakeCase,
		"tovalue": func(s string) string {
			return strings.TrimPrefix(s, "*")
		},
	}
)

func LoadBaseTemplateOnly(patterns ...string) func(*template.Template) *template.Template {
	return func(t *template.Template) *template.Template {
		filtered := []string{}
		for _, s := range patterns {
			if s != "" {
				filtered = append(filtered, s)
			}
		}
		if len(filtered) == 0 {
			return t
		}
		t, err := t.ParseFS(BaseTemplates, filtered...)
		if err != nil {
			panic(err)
		}
		return t
	}
}

func LoadBaseFuncs() func(*template.Template) *template.Template {
	return func(t *template.Template) *template.Template {
		t.Funcs(BaseFuncMap)
		return t
	}
}

func LoadBaseTemplate(patterns ...string) func(*template.Template) *template.Template {
	return func(t *template.Template) *template.Template {
		t = LoadBaseFuncs()(t)
		t = LoadBaseTemplateOnly(patterns...)(t)
		return t
	}
}

func createDict(values ...interface{}) (map[string]interface{}, error) {
	if len(values)%2 != 0 {
		return nil, errors.New("invalid dict call")
	}
	dict := make(map[string]interface{}, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			return nil, errors.New("dict keys must be strings")
		}
		dict[key] = values[i+1]
	}
	return dict, nil
}

func pluralizeString(plr *pluralize.Client) func(string) string {
	return func(s string) string {
		if plr.IsSingular(s) {
			s = plr.Plural(s)
		}
		return s
	}
}
