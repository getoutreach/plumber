// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for RenderContent error reporting of template sources.

package gen

import (
	"bytes"
	"strings"
	"testing"
	"testing/fstest"
	"text/template"

	"gotest.tools/v3/assert"
)

// renderErr renders templateName with the given options and returns the error message.
func renderErr(t *testing.T, templateName string, opts ...RenderOption) string {
	t.Helper()
	fm := WithFuncMap(template.FuncMap{"needs_string": func(s string) string { return s }})
	err := RenderContent(newTestContext(), templateName, &bytes.Buffer{}, struct{}{}, append([]RenderOption{fm}, opts...)...)
	assert.Assert(t, err != nil)
	return err.Error()
}

// assertContains fails the test when msg does not contain all the given parts.
func assertContains(t *testing.T, msg string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		assert.Assert(t, strings.Contains(msg, p), "expected %q in:\n%s", p, msg)
	}
}

func TestRenderContentNamedSourceInError(t *testing.T) {
	content := "{{/* header */}}\n{{define \"my/tpl\"}}\nline one\n\t{{ needs_string . }}\n{{end}}\n"
	msg := renderErr(t, "my/tpl", WithNamedTemplateContent("repo@main/tpl.gtpl", content))

	assert.Assert(t, !strings.Contains(msg, "root:"), msg)
	assertContains(t, msg,
		`template: repo@main/tpl.gtpl:4:17: executing "my/tpl"`,
		"--> repo@main/tpl.gtpl:4:17",
		"> 4 | \t{{ needs_string . }}\n",
		"| \t                ^\n",
	)
}

func TestRenderContentUnnamedSourceInError(t *testing.T) {
	msg := renderErr(t, "x", WithTemplateContent(`{{define "x"}}{{ needs_string . }}{{end}}`))
	assertContains(t, msg, "--> root:1:", `> 1 | {{define "x"}}{{ needs_string . }}{{end}}`)
}

func TestRenderContentFSSourceInError(t *testing.T) {
	fsys := fstest.MapFS{"templates/a.gtpl": {Data: []byte("{{define \"a\"}}\n{{ needs_string . }}\n{{end}}")}}
	msg := renderErr(t, "a", WithFS(fsys, "templates/a.gtpl"))
	assertContains(t, msg, "--> a.gtpl:2:16", "> 2 | {{ needs_string . }}")
}

func TestRenderContentParseErrorNamesSource(t *testing.T) {
	err := RenderContent(newTestContext(), "x", &bytes.Buffer{}, nil, WithNamedTemplateContent("content[bad]", "{{ if }}"))
	assert.ErrorContains(t, err, "content[bad]")
}

// newTestContext returns an initialized Context for rendering in tests.
func newTestContext() *Context {
	c := NewContext("app")
	c.Reset()
	return c
}
