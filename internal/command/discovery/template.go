// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Template rendering service for generating container files
// Managed: true

package discovery

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

// TemplateContext contains the data passed to container templates
type TemplateContext struct {
	PackageName string           `json:"package_name"`
	PackageFQN  string           `json:"package_fqn"`
	Container   ContainerContext `json:"container"`
	Config      ConfigContext    `json:"config"`
}

// ContainerContext contains container-specific template data
type ContainerContext struct {
	Name   string `json:"name"`
	Module string `json:"module"`
}

// ConfigContext contains config-specific template data
type ConfigContext struct {
	Type   string `json:"type"`
	Module string `json:"module"`
	Remote bool   `json:"remote"`
}

// TemplateRenderer handles rendering of container templates
type TemplateRenderer struct {
	templateStr string
}

// NewTemplateRenderer creates a new template renderer
func NewTemplateRenderer(templateStr string) *TemplateRenderer {
	return &TemplateRenderer{
		templateStr: templateStr,
	}
}

// RenderContainer renders a container file from template
func (r *TemplateRenderer) RenderContainer(
	containerPath string,
	containerName string,
	app *Application,
	sourceModule string,
) error {
	// Build template context as a map for easy template access
	ctx := r.buildContextMap(containerName, app, sourceModule)

	// Parse and execute template
	tmpl, err := template.New("plumber").Funcs(template.FuncMap{
		"print": fmt.Sprintf,
	}).Parse(r.templateStr)
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "plumber/command/discovery/container", ctx); err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(containerPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write rendered content
	if err := os.WriteFile(containerPath, buf.Bytes(), 0o600); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	// Note: goimports will be run after augmentation adds fields to the struct
	// Running it now would remove imports that will be needed after augmentation

	return nil
}

// buildContextMap creates the template context as a map for template execution
func (r *TemplateRenderer) buildContextMap(
	containerName string,
	app *Application,
	sourceModule string,
) map[string]interface{} {
	// Extract package name from application module
	packageName := extractPackageName(app.Module)

	// Parse config type and module
	configType, configModule, isRemote := parseConfigType(app.Config, app.Module)

	return map[string]interface{}{
		"package_name": packageName,
		"package_fqn":  app.Module,
		"container": map[string]interface{}{
			"name":   containerName,
			"module": sourceModule,
		},
		"config": map[string]interface{}{
			"type":   configType,
			"module": configModule,
			"remote": isRemote,
		},
	}
}

// extractPackageName extracts the package name from a module path
// Example: "github.com/getoutreach/plumber/example" -> "example"
func extractPackageName(modulePath string) string {
	if modulePath == "" {
		return "main"
	}
	parts := strings.Split(modulePath, "/")
	return parts[len(parts)-1]
}

// parseConfigType parses the config type string and extracts module info
// Handles formats like:
// - "*Config" -> ("*Config", "", false)
// - "*\"github.com/getoutreach/plumber/example\".Config" -> ("*Config", "github.com/...", true)
func parseConfigType(configStr, appModule string) (typeName, module string, remote bool) {
	if configStr == "" {
		return "*Config", "", false
	}

	// Check if it contains a quoted module path
	if strings.Contains(configStr, "\"") {
		// Extract the module path from quotes
		start := strings.Index(configStr, "\"")
		end := strings.LastIndex(configStr, "\"")
		if start != -1 && end != -1 && end > start {
			module = configStr[start+1 : end]

			// Extract type name (everything before the quotes and after)
			prefix := configStr[:start]
			suffix := configStr[end+1:]

			// Skip the dot separator after the quoted module
			if suffix != "" && suffix[0] == '.' {
				suffix = suffix[1:]
			}

			typeName = prefix + suffix

			// Check if remote
			remote = (module != appModule)
			return typeName, module, remote
		}
	}

	// Simple type like "*Config"
	return configStr, "", false
}
