// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file define an expansion of transformer output
package expand

import (
	"bytes"
	"fmt"
	"path"
	"strings"
	"text/template"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/query/model"
)

// outputTemplateData is the template context used to expand the value of the
// plumber:output annotation. It exposes the source-file identity components
// commonly needed for naming generated files:
//
//   - .Filename — the full base filename of the source file (e.g. "model.go").
//   - .Name     — the source filename without extension (e.g. "model").
//   - .Ext      — the source file extension including the leading dot (".go").
//
// In addition, the template environment registers a `suffixed` function that
// produces "<.Name>_<suffix><.Ext>" — the equivalent of the legacy
// `{suffix:<str>}` placeholder. Example: `{{ suffixed "filter" }}` evaluates
// to `model_filter.go` when expanding output for `model.go`.
type outputTemplateData struct {
	Filename string
	Name     string
	Ext      string
}

// TransformerOutput renders the final output filename for a transformation by
// evaluating the value of the plumber:output annotation as a Go text/template
// against the source file's identity (.Filename, .Name, .Ext). The template
// environment registers a `suffixed` function so callers can write
// `{{ suffixed "filter" }}` to produce `<name>_filter<ext>`.
//
// When no plumber:output annotation is present, the default `generated.go`
// is returned. If the value contains no `{{` template delimiters it is
// treated as a literal filename and returned verbatim — this keeps simple
// values like `merged.go` working without any template overhead.
//
// Template execution errors fall back to returning the raw annotation value;
// they do not panic. Authors are expected to validate their templates via the
// usual generation pipeline (where the rendered filename is exercised by
// every run).
func TransformerOutput(annotations model.Annotations, fileName string) string {
	output := "generated.go"

	a := annotations.Find(contract.OptionOutput)
	if a != nil {
		output = a.Value()
	}

	baseFilename := path.Base(fileName)
	ext := path.Ext(baseFilename)
	name := strings.TrimSuffix(baseFilename, ext)

	rendered, err := renderOutputTemplate(output, outputTemplateData{
		Filename: baseFilename,
		Name:     name,
		Ext:      ext,
	})
	if err != nil {
		return output
	}
	return rendered
}

// renderOutputTemplate parses and executes value as a Go text/template using
// the supplied data. Strings without `{{` template delimiters are returned
// unchanged so that plain filenames incur no template overhead and behave
// identically to the previous regex-based implementation.
//
// The template environment includes a `suffixed` function:
//
//	{{ suffixed "filter" }} → "<.Name>_filter<.Ext>"
//
// which is the modern replacement for the legacy `{suffix:<str>}` placeholder.
func renderOutputTemplate(value string, data outputTemplateData) (string, error) {
	if !strings.Contains(value, "{{") {
		return value, nil
	}

	funcs := template.FuncMap{
		"suffixed": func(suffix string) string {
			return fmt.Sprintf("%s_%s%s", data.Name, suffix, data.Ext)
		},
	}

	tmpl, err := template.New("plumber:output").Option("missingkey=error").Funcs(funcs).Parse(value)
	if err != nil {
		return "", fmt.Errorf("parsing plumber:output template %q: %w", value, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("executing plumber:output template %q: %w", value, err)
	}
	return buf.String(), nil
}
