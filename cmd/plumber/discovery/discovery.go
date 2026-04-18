// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Main discovery command runner
// Managed: true

// Package discovery implements the discovery command which analyzes container files to discover providers and augments the
// container structs with missing provider fields.
package discovery

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/getoutreach/plumber/internal/command"
	"github.com/getoutreach/plumber/internal/command/discovery"
	"github.com/getoutreach/plumber/internal/command/discovery/contract"
	"github.com/urfave/cli/v2"
)

// Run executes the discovery command
func Run(c *cli.Context) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", r)
			os.Exit(1)
		}
	}()
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
	cfg, err := command.ParseConfig[discovery.Config](absConfigPath)
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

// containerInfo holds information about a container, including its configuration, file path, and associated source
// file paths for discovery.
type containerInfo struct {
	config        *discovery.PlumberContainerConfig
	containerPath string
	sourcePaths   []string
}

// discoveredProviders holds the discovered providers for a container, along with a reference to the container
// information and a map of provider mappings for augmentation.
type discoveredProviders struct {
	container   *containerInfo
	providers   []*contract.Provider
	providerMap map[string]*contract.ProviderMapping
}

func processApplication(ctx context.Context, baseDir string, app *discovery.Application, cfg *discovery.Config) error {
	// Collect container information and file paths
	containers, allPaths := collectContainerInfo(baseDir, app, cfg)
	if len(containers) == 0 {
		fmt.Printf("  No valid containers to process\n")
		return nil
	}

	// Create a map to hold the provider mappings
	var providerMap = make(map[string]*contract.ProviderMapping)

	// Create AST parser once for all paths (loads all packages together)
	fmt.Printf("\n  Loading %d file(s) across %d container(s) for analysis...\n", len(allPaths), len(containers))
	astParser, err := discovery.NewASTParser(allPaths...)
	if err != nil {
		return fmt.Errorf("failed to create AST parser: %w", err)
	}

	// Phase 1: Discover providers from all containers
	fmt.Printf("\n  Phase 1: Discovering providers...\n")
	allDiscovered := make([]*discoveredProviders, 0, len(containers))
	for _, info := range containers {
		discovered, err := discoverContainerProviders(astParser, info)
		if err != nil {
			return err
		}
		// Populate the provider map
		for _, provider := range discovered.providers {
			providerType := provider.Type.TypeInfo.Type.String()
			if _, exists := providerMap[providerType]; !exists {
				providerMap[providerType] = &contract.ProviderMapping{Type: provider.Type.TypeInfo.Type, Providers: []*contract.ContainerProvider{}}
			}
			providerMap[providerType].Providers = append(providerMap[providerType].Providers, &contract.ContainerProvider{
				ContainerName: info.config.Name,
				Provider:      provider,
			})
		}
		discovered.providerMap = providerMap
		allDiscovered = append(allDiscovered, discovered)
	}

	// Phase 2: Augment containers with discovered providers
	fmt.Printf("\n  Phase 2: Augmenting containers...\n")
	for _, discovered := range allDiscovered {
		if err := augmentContainer(discovered); err != nil {
			return err
		}
	}

	return nil
}

func collectContainerInfo(
	baseDir string, app *discovery.Application, cfg *discovery.Config,
) (containers []*containerInfo, allPaths []string) {
	containers = make([]*containerInfo, 0)

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

// discoverContainerProviders discovers providers from a container's source files
// Function definition corrected
func discoverContainerProviders(astParser *discovery.ASTParser, info *containerInfo) (*discoveredProviders, error) {
	cfg := info.config
	fmt.Printf("\n  Container: %s\n", cfg.Name)
	fmt.Printf("    Container path: %s\n", info.containerPath)

	discovered := &discoveredProviders{
		container: info,
		providers: []*contract.Provider{},
	}

	// Process source files for discovery with matchers
	if len(info.sourcePaths) > 0 {
		result, err := discoverSourceTypes(astParser, info.sourcePaths, cfg)
		if err != nil {
			return nil, err
		}

		discovered.providers = result.Providers

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
		}
	} else {
		fmt.Printf("    No source paths configured\n")
	}

	return discovered, nil
}

// augmentContainer augments a container with its discovered providers
func augmentContainer(discovered *discoveredProviders) error {
	info := discovered.container
	cfg := info.config

	fmt.Printf("\n  Container: %s\n", cfg.Name)

	if len(discovered.providers) == 0 {
		fmt.Printf("    No providers to augment\n")
		return nil
	}

	// Augment the container struct with missing provider fields
	if err := augmentContainerWithProviders(info.containerPath, cfg.Name, discovered.providers, discovered.providerMap); err != nil {
		fmt.Printf("    ⚠ Warning: Failed to augment container: %v\n", err)
		return nil // Don't fail the entire process
	}

	fmt.Printf("    ✓ Container augmented with discovered providers\n")

	// Run goimports to fix imports
	if err := runGoimports(info.containerPath); err != nil {
		fmt.Printf("    ⚠ Warning: Failed to run goimports: %v\n", err)
	}

	return nil
}

func discoverSourceTypes(
	astParser *discovery.ASTParser,
	sourcePaths []string,
	cfg *discovery.PlumberContainerConfig,
) (*contract.DiscoveryResult, error) {
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
func augmentContainerWithProviders(
	containerPath, containerName string,
	providers []*contract.Provider,
	providerMap map[string]*contract.ProviderMapping,
) error {
	// Create a new parser specifically for the container file
	parser, err := discovery.NewASTParser(containerPath)
	if err != nil {
		return fmt.Errorf("failed to create parser for container: %w", err)
	}

	// Get the file and decorator from the parser
	file, pkg, dec := parser.GetFileAndDecorator(containerPath)
	if file == nil {
		return fmt.Errorf("failed to get AST for container file")
	}

	// Create augmenter
	augmenter := discovery.NewAugmenter()

	// Augment the container struct
	result, err := augmenter.AugmentContainerStruct(pkg, containerPath, containerName, providers, file, dec, providerMap)
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
	return nil
}

// renderContainerFromTemplate renders a container file from template
func renderContainerFromTemplate(
	containerPath string,
	containerName string,
	app *discovery.Application,
	sourceModule string,
	cfg *discovery.Config,
) error {
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
