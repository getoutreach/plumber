// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file renders embedded skill files through Go text/template
// while exposing helpers that inject the output of the shape describe subcommand
// (macros, options, handlers, and template functions) into installed skills.

package skills

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"text/template"

	"github.com/getoutreach/plumber/internal/command/shape/describe"
)

// TemplateContext bundles everything a skill template needs from the shape
// configuration. Population is deferred to callers so the skills package
// stays independent of CLI wiring.
type TemplateContext struct {
	// Description holds macros, options, and handlers (output of describe.Build).
	Description describe.Description
	// Functions holds template-function descriptions (output of describe.BuildFunctions).
	Functions describe.FunctionsDescription
	// Structures holds structure descriptions (output of describe.BuildStructures).
	Structures describe.StructuresDescription
}

// Render evaluates content as a Go text/template using ctx and returns the
// produced bytes. Skill templates use the custom delimiter pair "[[" / "]]"
// so that example Go template snippets (which use "{{" / "}}") inside skill
// markdown can be quoted verbatim without escaping. When content does not
// include the opening delimiter the input is returned verbatim.
func Render(name string, content []byte, ctx TemplateContext) ([]byte, error) {
	if !bytes.Contains(content, []byte("[[")) {
		return content, nil
	}
	fm, err := funcMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("building template function map: %w", err)
	}

	tmpl, err := template.New(name).
		Delims("[[", "]]").
		Funcs(fm).
		Parse(string(content))
	if err != nil {
		return nil, fmt.Errorf("parsing skill template %q: %w", name, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, ctx); err != nil {
		return nil, fmt.Errorf("executing skill template %q: %w", name, err)
	}
	return buf.Bytes(), nil
}

// funcMap returns the function map exposed to skill templates. All helpers
// produce markdown so generated content slots cleanly into existing SKILL.md
// files.
func funcMap(ctx TemplateContext) (template.FuncMap, error) {
	mdFormatter, mdFormatterErr := describe.Format(describe.FormatMD)
	mdFunctionsFormatter, mdFunctionsFormatterErr := describe.FunctionsFormat(describe.FormatMD)
	mdStructuresFormatter, mdStructuresFormatterErr := describe.StructuresFormat(describe.FormatMD)

	if mdFormatterErr != nil || mdFunctionsFormatterErr != nil || mdStructuresFormatterErr != nil {
		// If any of the formatters fail to initialize, return an empty function map.
		// This prevents panics during template execution while still allowing templates
		// that don't use the describe functions to render.
		return template.FuncMap{}, errors.Join(mdFormatterErr, mdFunctionsFormatterErr, mdStructuresFormatterErr)
	}

	formatDesc := func(d describe.Description) (string, error) {
		out, err := mdFormatter.Format(d)
		if err != nil {
			return "", err
		}
		return string(out), nil
	}

	return template.FuncMap{
		// describe renders the entire describe output (macros + options + handlers) as markdown.
		"describe": func() (string, error) {
			return formatDesc(ctx.Description)
		},
		// describeMacros renders only the macro section as markdown.
		"describeMacros": func() (string, error) {
			return formatDesc(describe.Description{Macros: ctx.Description.Macros})
		},
		// describeOptions renders only the option section as markdown.
		"describeOptions": func() (string, error) {
			return formatDesc(describe.Description{Options: ctx.Description.Options})
		},
		// describeHandlers renders only the handler section as markdown.
		"describeHandlers": func() (string, error) {
			return formatDesc(describe.Description{Handlers: ctx.Description.Handlers})
		},
		// describeMacro renders a single macro by name as markdown. Returns an
		// empty string when the macro is unknown.
		"describeMacro": func(name string) (string, error) {
			for _, m := range ctx.Description.Macros {
				if m.Name == name {
					return formatDesc(describe.Description{Macros: []describe.MacroDescription{m}})
				}
			}
			return "", nil
		},
		// describeOption renders a single option by name as markdown. Returns
		// an empty string when the option is unknown.
		"describeOption": func(name string) (string, error) {
			for _, o := range ctx.Description.Options {
				if o.Name == name {
					return formatDesc(describe.Description{Options: []describe.OptionDescription{o}})
				}
			}
			return "", nil
		},
		// describeFunctions renders all template-function descriptions as markdown.
		"describeFunctions": func() (string, error) {
			out, err := mdFunctionsFormatter.FormatFunctions(ctx.Functions)
			if err != nil {
				return "", err
			}
			return string(out), nil
		},
		// describeStructures renders all registered structures and their paths as markdown.
		"describeStructures": func() (string, error) {
			out, err := mdStructuresFormatter.FormatStructures(ctx.Structures)
			if err != nil {
				return "", err
			}
			return string(out), nil
		},
		// indent prefixes every non-empty line of s with n spaces. Convenient for
		// embedding multi-line markdown inside list items.
		"indent": func(n int, s string) string {
			pad := strings.Repeat(" ", n)
			lines := strings.Split(s, "\n")
			for i, l := range lines {
				if l != "" {
					lines[i] = pad + l
				}
			}
			return strings.Join(lines, "\n")
		},
	}, nil
}
