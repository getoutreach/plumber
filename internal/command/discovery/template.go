// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Template rendering service for generating container files
// Managed: true

package discovery

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/getoutreach/plumber/internal/command/discovery/render"
	"github.com/getoutreach/plumber/internal/genius/gen"
	baserender "github.com/getoutreach/plumber/internal/render"
	"github.com/getoutreach/plumber/query/model"
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

// TemplateRenderer handles rendering of container and application templates
type TemplateRenderer struct {
	containerOpts   []gen.RenderOptionsFunc
	applicationOpts []gen.RenderOptionsFunc
}

// NewTemplateRenderer creates a new template renderer with separate container and application render options.
// Global options should be merged into both slices by the caller.
func NewTemplateRenderer(containerOpts, applicationOpts []gen.RenderOptionsFunc) *TemplateRenderer {
	return &TemplateRenderer{
		containerOpts:   containerOpts,
		applicationOpts: applicationOpts,
	}
}

// RenderContainer renders a container file from template
func (r *TemplateRenderer) RenderContainer(
	containerPath string,
	containerName string,
	app *Application,
	sourceModule string,
) error {
	var (
		ctx = r.buildContext(containerPath, app, sourceModule)
		// Build template context as a map for easy template access
		scope = r.buildScope(containerName, app, sourceModule)
	)

	// Ensure directory exists
	dir := filepath.Dir(containerPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Rendered content
	if err := render.Render(
		ctx,
		containerPath, "plumber/command/discovery/container",
		scope, gen.NewSystemFileOpener(), r.containerOpts...,
	); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	// Note: goimports will be run after augmentation adds fields to the struct
	// Running it now would remove imports that will be needed after augmentation

	return nil
}

// RenderApplication renders an application file from template
func (r *TemplateRenderer) RenderApplication(
	applicationPath string,
	containerName string,
	app *Application,
	sourceModule string,
) error {
	var (
		ctx   = r.buildContext(applicationPath, app, sourceModule)
		scope = r.buildScope(containerName, app, sourceModule)
	)

	// Ensure directory exists
	dir := filepath.Dir(applicationPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if err := render.Render(
		ctx,
		applicationPath, "plumber/command/discovery/application",
		scope, gen.NewSystemFileOpener(), r.applicationOpts...,
	); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func (r *TemplateRenderer) buildContext(output string, app *Application, sourceModule string) baserender.Context {
	return baserender.NewRenderContext(
		baserender.NewModuleRegister(),
		&model.Package{
			Name: path.Base(sourceModule),
			Path: sourceModule,
		},
		output,
	)
}

// buildContextMap creates the template context as a map for template execution
func (r *TemplateRenderer) buildScope(
	containerName string,
	app *Application,
	sourceModule string,
) map[string]interface{} {
	// Extract package name from application module
	packageName := extractPackageName(app.Module)

	// Parse config type and module
	// configType, configModule, isRemote := parseConfigType(app.Config, app.Module)

	return map[string]interface{}{
		"Container": map[string]interface{}{
			"Name":   containerName,
			"Module": sourceModule,
		},
		"Config":       model.TypeSpec{FQN: app.Config},
		"package_name": packageName,
		"package_fqn":  app.Module,
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
