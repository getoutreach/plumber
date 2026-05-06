// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the structure expansion logic for structure related template variables
package expand

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/render"
)

func Structure(cfg *config.PlumberStructureConfig, repoModule, module contract.ModuleInfo) (err error) {
	scope := render.Scope{
		"Repo":   repoModule,
		"Module": module,
	}
	cfg.Path, err = expandStructureValue(scope, "path", cfg.Path)
	if err != nil {
		return fmt.Errorf("expanding structure path: %w", err)
	}
	for i, p := range cfg.Paths {
		expandedPath, err := expandStructureValue(scope, fmt.Sprintf("structure.path[%d]", i), p.Path.Path)
		if err != nil {
			return fmt.Errorf("expanding structure path for path %q: %w", p.Path.Name, err)
		}
		cfg.Paths[i].Path.Path = expandedPath
	}
	return nil
}

// expandTemplateStr parses and executes s as a text/template against data.
// If s contains no template delimiters it is returned as-is.
func expandStructureValue(scope render.Scope, name, s string) (string, error) {
	if !strings.Contains(s, "{{") {
		return s, nil
	}

	tmpl, err := template.New(name).
		Option("missingkey=error").
		Funcs(render.GenericFunctions()).
		Funcs(map[string]any{}).Parse(s)

	if err != nil {
		return "", fmt.Errorf("parsing template %q: %w", s, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, scope); err != nil {
		return "", fmt.Errorf("executing template %q: %w", s, err)
	}

	return buf.String(), nil
}
