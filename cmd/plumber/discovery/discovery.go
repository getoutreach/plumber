// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Main discovery command runner
// Managed: true

package discovery

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/getoutreach/plumber/internal/discovery"
	"github.com/getoutreach/plumber/internal/discovery/contract"
	"github.com/urfave/cli/v2"
)

// Run executes the discovery command
func Run(c *cli.Context) error {
	ctx := c.Context
	configPath := c.String("config")

	// Resolve absolute path for config file
	absConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return fmt.Errorf("failed to resolve config path: %w", err)
	}

	// Use config file's directory as base directory
	baseDir := filepath.Dir(absConfigPath)

	// Parse the configuration
	cfg, err := discovery.ParseConfig(absConfigPath)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Process loops to expand containers
	if err := discovery.ProcessLoops(cfg, baseDir); err != nil {
		return fmt.Errorf("failed to process loops: %w", err)
	}

	// Print expanded configuration
	fmt.Println("Expanded Configuration:")
	fmt.Println("=======================")
	discovery.PrintConfig(os.Stdout, cfg)

	// Process each application
	for _, app := range cfg.Applications {
		fmt.Printf("\nProcessing application: %s\n", app.Name)
		fmt.Println(strings.Repeat("-", 50))

		if err := processApplication(ctx, baseDir, &app, cfg); err != nil {
			return fmt.Errorf("failed to process application %q: %w", app.Name, err)
		}
	}

	return nil
}

type containerInfo struct {
	config        *discovery.PlumberContainerConfig
	containerPath string
	sourcePaths   []string
}

func processApplication(ctx context.Context, baseDir string, app *discovery.Application, cfg *discovery.Config) error {
	// Collect container information and file paths
	containers, allPaths := collectContainerInfo(baseDir, app, cfg)
	if len(containers) == 0 {
		fmt.Printf("  No valid containers to process\n")
		return nil
	}

	// Create AST parser once for all paths (loads all packages together)
	fmt.Printf("\n  Loading %d file(s) across %d container(s) for analysis...\n", len(allPaths), len(containers))
	astParser, err := discovery.NewASTParser(allPaths...)
	if err != nil {
		return fmt.Errorf("failed to create AST parser: %w", err)
	}

	// Process each container
	for _, info := range containers {
		if err := processContainer(astParser, info); err != nil {
			return err
		}
	}

	return nil
}

func collectContainerInfo(baseDir string, app *discovery.Application, cfg *discovery.Config) ([]*containerInfo, []string) {
	var allPaths []string
	containers := make([]*containerInfo, 0)

	for _, container := range app.Containers {
		if container.PlumberContainer == nil {
			continue
		}

		containerCfg := container.PlumberContainer
		info := &containerInfo{
			config:      containerCfg,
			sourcePaths: []string{},
		}

		// Resolve container path
		containerPath := containerCfg.Container.Path
		if !filepath.IsAbs(containerPath) {
			containerPath = filepath.Join(baseDir, containerPath)
		}

		// Check if container file exists
		if _, err := os.Stat(containerPath); os.IsNotExist(err) {
			fmt.Printf("\n  Container: %s\n", containerCfg.Name)

			// Try to render from template if available
			if containerCfg.Source != nil {
				sourceModule := getSourceModulePath(baseDir, containerCfg.Source.Path, app.Module)

				if err := renderContainerFromTemplate(containerPath, containerCfg.Name, app, sourceModule, cfg); err != nil {
					fmt.Printf("    ⚠ Warning: Failed to render template: %v\n", err)
					fmt.Printf("    ⚠ Warning: Container file does not exist at %s (skipping)\n", containerPath)
					continue
				}

				fmt.Printf("    ✓ Container file created from template at %s\n", containerPath)
			} else {
				fmt.Printf("    ⚠ Warning: Container file does not exist at %s (skipping)\n", containerPath)
				continue
			}
		}

		info.containerPath = containerPath
		allPaths = append(allPaths, containerPath)

		// Resolve source paths if specified
		if containerCfg.Source != nil {
			sourcePaths := resolveSourcePaths(baseDir, containerCfg)
			info.sourcePaths = sourcePaths
			allPaths = append(allPaths, sourcePaths...)
		}

		containers = append(containers, info)
	}

	return containers, allPaths
}

func resolveSourcePaths(baseDir string, cfg *discovery.PlumberContainerConfig) []string {
	var sourcePaths []string

	sourcePath := cfg.Source.Path
	if !filepath.IsAbs(sourcePath) {
		sourcePath = filepath.Join(baseDir, sourcePath)
	}

	// Check if source is a directory
	fileInfo, err := os.Stat(sourcePath)
	if err != nil {
		fmt.Printf("\n  Container: %s\n", cfg.Name)
		fmt.Printf("    ⚠ Warning: Source path does not exist at %s (skipping sources)\n", sourcePath)
		return sourcePaths
	}

	if fileInfo.IsDir() {
		// Read all .go files in the directory
		entries, err := os.ReadDir(sourcePath)
		if err != nil {
			fmt.Printf("    ⚠ Warning: Failed to read source directory %q: %v\n", sourcePath, err)
			return sourcePaths
		}

		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) == ".go" {
				goFile := filepath.Join(sourcePath, entry.Name())
				sourcePaths = append(sourcePaths, goFile)
			}
		}
	} else {
		// Single file
		sourcePaths = append(sourcePaths, sourcePath)
	}

	return sourcePaths
}

func processContainer(astParser *discovery.ASTParser, info *containerInfo) error {
	cfg := info.config
	fmt.Printf("\n  Container: %s\n", cfg.Name)
	fmt.Printf("    Container path: %s\n", info.containerPath)

	// Process source files for discovery with matchers
	if len(info.sourcePaths) > 0 {
		result, err := discoverSourceTypes(astParser, info.sourcePaths, cfg)
		if err != nil {
			return err
		}

		// Display discovered providers
		if len(result.Providers) > 0 {
			fmt.Printf("\n    Providers:\n")
			for _, provider := range result.Providers {
				typeName := "unknown"
				if provider.Type != nil {
					typeName = provider.Type.TypeInfo.Type.String()
				}
				fmt.Printf("      %s\n", provider.Name)
				fmt.Printf("        Type: %s\n", typeName)
				if provider.Constructor != nil {
					fmt.Printf("        Func: %s\n", provider.Constructor.FunctionName)
				}
			}

			// Augment the container struct with missing provider fields
			if err := augmentContainerWithProviders(info.containerPath, cfg.Name, result.Providers); err != nil {
				fmt.Printf("    ⚠ Warning: Failed to augment container: %v\n", err)
			} else {
				fmt.Printf("    ✓ Container augmented with discovered providers\n")

				// Run goimports to fix imports
				if err := runGoimports(info.containerPath); err != nil {
					fmt.Printf("    ⚠ Warning: Failed to run goimports: %v\n", err)
				}
			}
		}
	} else {
		fmt.Printf("    No source paths configured\n")
	}

	return nil
}

func discoverSourceTypes(astParser *discovery.ASTParser, sourcePaths []string, cfg *discovery.PlumberContainerConfig) (*contract.DiscoveryResult, error) {
	fmt.Printf("    Source path(s): %d file(s)\n", len(sourcePaths))

	// Create file filter for source files
	sourceFilter := make(map[string]bool)
	for _, path := range sourcePaths {
		sourceFilter[path] = true
	}

	// Discover types and constructors in source files
	result, err := astParser.DiscoverInFiles(cfg.Matchers, sourceFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to discover types in sources for %q: %w", cfg.Name, err)
	}

	// Print discovered information
	fmt.Printf("    Discovered in sources:\n")
	fmt.Printf("      Providers: %d\n", len(result.Providers))
	for _, provider := range result.Providers {
		typeName := "unknown"
		if provider.Type != nil {
			typeName = provider.Type.TypeName
		}
		fmt.Printf("        - %s (Type: %s, %d constructor(s))\n",
			provider.Name, typeName, 1)
	}

	return result, nil
}

// augmentContainerWithProviders adds missing provider fields to the container struct
func augmentContainerWithProviders(containerPath, containerName string, providers []*contract.Provider) error {
	// Create a new parser specifically for the container file
	parser, err := discovery.NewASTParser(containerPath)
	if err != nil {
		return fmt.Errorf("failed to create parser for container: %w", err)
	}

	// Get the file and decorator from the parser
	file, dec := parser.GetFileAndDecorator(containerPath)
	if file == nil {
		return fmt.Errorf("failed to get AST for container file")
	}

	// Create augmenter
	augmenter := discovery.NewAugmenter()

	// Augment the container struct
	result, err := augmenter.AugmentContainerStruct(containerPath, containerName, providers, file, dec)
	if err != nil {
		return err
	}

	// Report what was added
	if len(result.Added) > 0 {
		fmt.Printf("      Added fields: %s\n", strings.Join(result.Added, ", "))
	}
	if len(result.Skipped) > 0 {
		fmt.Printf("      Skipped existing fields: %s\n", strings.Join(result.Skipped, ", "))
	}

	return nil
}

// runGoimports runs goimports on a file to clean up imports
func runGoimports(filePath string) error {
	// First run gofmt to ensure proper formatting
	cmd := exec.Command("gofmt", "-w", filePath)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("gofmt failed: %w\nOutput: %s", err, string(output))
	}

	// Then run goimports to organize imports
	cmd = exec.Command("goimports", "-w", filePath)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("goimports failed: %w\nOutput: %s", err, string(output))
	}
	return nil
}

// extractPlumberType extracts the wrapped type from plumber.D[T] or plumber.R[T]
// Returns empty string if not a plumber wrapper type
func extractPlumberType(typeStr string) string {
	// Match plumber.D[...] or plumber.R[...]
	if strings.HasPrefix(typeStr, "plumber.D[") || strings.HasPrefix(typeStr, "plumber.R[") {
		start := strings.Index(typeStr, "[")
		end := strings.LastIndex(typeStr, "]")
		if start != -1 && end != -1 && end > start {
			inner := typeStr[start+1 : end]
			// Remove pointer prefix if present
			return strings.TrimPrefix(inner, "*")
		}
	}
	return ""
}

// typesMatch checks if a container field type matches a discovered struct
// Handles package prefixes and pointer types
func typesMatch(fieldType, structName string) bool {
	// Remove package prefix from field type (e.g., "database.Repository" -> "Repository")
	parts := strings.Split(fieldType, ".")
	if len(parts) > 0 {
		fieldBaseName := parts[len(parts)-1]
		return fieldBaseName == structName
	}
	return fieldType == structName
}

// isStructType checks if a type looks like a struct (not a primitive)
// Primitives: int, int32, string, bool, etc.
// Structs: SomeName, package.SomeName, *SomeName
func isStructType(typeStr string) bool {
	// Remove pointer prefix
	typeStr = strings.TrimPrefix(typeStr, "*")

	// If it has a package prefix (contains .), it's a struct
	if strings.Contains(typeStr, ".") {
		return true
	}

	// If it starts with uppercase and is not a known primitive, it's likely a struct
	if len(typeStr) > 0 && typeStr[0] >= 'A' && typeStr[0] <= 'Z' {
		// Check for known primitive types that are capitalized
		primitives := map[string]bool{
			"String":  true,
			"Int":     true,
			"Int8":    true,
			"Int16":   true,
			"Int32":   true,
			"Int64":   true,
			"Uint":    true,
			"Uint8":   true,
			"Uint16":  true,
			"Uint32":  true,
			"Uint64":  true,
			"Bool":    true,
			"Float32": true,
			"Float64": true,
		}
		return !primitives[typeStr]
	}

	// Lowercase types are primitives (int, string, bool, etc.)
	return false
}

// renderContainerFromTemplate renders a container file from template
func renderContainerFromTemplate(
	containerPath string,
	containerName string,
	app *discovery.Application,
	sourceModule string,
	cfg *discovery.Config,
) error {
	// Check if template is available
	if cfg.Templates.Container == "" {
		return fmt.Errorf("no container template defined in configuration")
	}

	// Create template renderer
	renderer := discovery.NewTemplateRenderer(cfg.Templates.Container)

	// Render the container file
	return renderer.RenderContainer(containerPath, containerName, app, sourceModule)
}

// getSourceModulePath determines the module path for the source package
func getSourceModulePath(baseDir, sourcePath, appModule string) string {
	if appModule == "" {
		return ""
	}

	// Resolve source path to absolute
	absSourcePath := sourcePath
	if !filepath.IsAbs(sourcePath) {
		absSourcePath = filepath.Join(baseDir, sourcePath)
	}

	// Get the relative path from baseDir
	relPath, err := filepath.Rel(baseDir, absSourcePath)
	if err != nil {
		return appModule
	}

	// Clean up the relative path
	relPath = filepath.ToSlash(relPath)

	// Build module path by appending relative path to app module
	if relPath != "" && relPath != "." {
		return appModule + "/" + relPath
	}

	return appModule
}
