// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Main discovery command runner
// Managed: true

package discovery

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/getoutreach/plumber/internal/discovery"
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
	fmt.Println("=======================\n")
	discovery.PrintConfig(os.Stdout, cfg)

	// Process each application
	for _, app := range cfg.Applications {
		fmt.Printf("\nProcessing application: %s\n", app.Name)
		fmt.Println(strings.Repeat("-", 50))

		if err := processApplication(ctx, baseDir, &app); err != nil {
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

func processApplication(ctx context.Context, baseDir string, app *discovery.Application) error {
	// Collect container information and file paths
	containers, allPaths := collectContainerInfo(baseDir, app)
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

func collectContainerInfo(baseDir string, app *discovery.Application) ([]*containerInfo, []string) {
	var allPaths []string
	containers := make([]*containerInfo, 0)

	for _, container := range app.Containers {
		if container.PlumberContainer == nil {
			continue
		}

		cfg := container.PlumberContainer
		info := &containerInfo{
			config:      cfg,
			sourcePaths: []string{},
		}

		// Resolve container path
		containerPath := cfg.Container.Path
		if !filepath.IsAbs(containerPath) {
			containerPath = filepath.Join(baseDir, containerPath)
		}

		// Check if container file exists
		if _, err := os.Stat(containerPath); os.IsNotExist(err) {
			fmt.Printf("\n  Container: %s\n", cfg.Name)
			fmt.Printf("    ⚠ Warning: Container file does not exist at %s (skipping)\n", containerPath)
			continue
		}

		info.containerPath = containerPath
		allPaths = append(allPaths, containerPath)

		// Resolve source paths if specified
		if cfg.Source != nil {
			sourcePaths := resolveSourcePaths(baseDir, cfg)
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

	// Verify that container file has a struct matching the module name
	containerStruct := verifyContainerStruct(astParser, info.containerPath, cfg.Name)

	// Process source files for discovery with matchers
	if len(info.sourcePaths) > 0 {
		result, err := discoverSourceTypes(astParser, info.sourcePaths, cfg)
		if err != nil {
			return err
		}

		// Match container fields with discovered structs
		if containerStruct != nil {
			matchContainerFields(containerStruct, result)

			// Augment container struct with missing fields (uses already-parsed AST)
			if err := augmentContainerStruct(astParser, info.containerPath, cfg.Name, containerStruct, result); err != nil {
				return fmt.Errorf("failed to augment container struct: %w", err)
			}
		}
	} else {
		fmt.Printf("    No source paths configured\n")
	}

	return nil
}

func verifyContainerStruct(astParser *discovery.ASTParser, containerPath string, containerName string) *discovery.StructInfo {
	containerFilter := map[string]bool{containerPath: true}
	containerResult, err := astParser.DiscoverInFiles(nil, containerFilter)
	if err != nil {
		fmt.Printf("    ⚠ Warning: Failed to analyze container file: %v\n", err)
		return nil
	}

	// Check for struct matching the container name
	for _, s := range containerResult.Structs {
		if s.Name == containerName {
			fmt.Printf("    ✓ Container struct found: %s (%d fields)\n", s.Name, len(s.Fields))
			return s
		}
	}

	fmt.Printf("    ⚠ Warning: No struct named %q found in container file\n", containerName)
	return nil
}

func discoverSourceTypes(astParser *discovery.ASTParser, sourcePaths []string, cfg *discovery.PlumberContainerConfig) (*discovery.DiscoveryResult, error) {
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
	fmt.Printf("      Structs: %d\n", len(result.Structs))
	for _, s := range result.Structs {
		fmt.Printf("        - %s (%d fields)\n", s.Name, len(s.Fields))
	}

	fmt.Printf("      Constructors: %d\n", len(result.Constructors))
	for _, c := range result.Constructors {
		fmt.Printf("        - %s() -> %s\n", c.Name, c.ReturnType)
	}

	return result, nil
}

func augmentContainerStruct(astParser *discovery.ASTParser, containerPath string, containerName string, containerStruct *discovery.StructInfo, result *discovery.DiscoveryResult) error {
	// Get the already-parsed AST file from the parser
	file, err := astParser.GetParsedFile(containerPath)
	if err != nil {
		return fmt.Errorf("failed to get parsed file: %w", err)
	}

	// Get the file set
	fset := astParser.GetFileSet()

	// Augment using the parsed AST
	augmenter := discovery.NewAugmenter()
	augmentResult, err := augmenter.AugmentContainerStruct(containerPath, containerName, containerStruct, result, file, fset)
	if err != nil {
		return err
	}

	if len(augmentResult.Added) > 0 {
		fmt.Printf("\n    Augmentation:\n")
		fmt.Printf("      Added %d field(s):\n", len(augmentResult.Added))
		for _, fieldName := range augmentResult.Added {
			fmt.Printf("        + %s\n", fieldName)
		}
	}

	return nil
}

// matchContainerFields compares container struct fields with discovered source structs
func matchContainerFields(container *discovery.StructInfo, sourceResult *discovery.DiscoveryResult) {
	if len(container.Fields) == 0 {
		return
	}

	fmt.Printf("\n    Field Matching:\n")

	// Build a map of discovered struct names for quick lookup
	sourceStructs := make(map[string]*discovery.StructInfo)
	for _, s := range sourceResult.Structs {
		sourceStructs[s.Name] = s
	}

	matched := 0
	missing := 0
	extra := 0

	for _, field := range container.Fields {
		// Extract the type from plumber wrappers: plumber.D[T] or plumber.R[T]
		fieldType := extractPlumberType(field.TypeName)
		if fieldType == "" {
			continue // Skip non-plumber fields
		}

		// Check if a struct with matching name exists in source
		if sourceStruct, found := sourceStructs[field.Name]; found {
			// Check if types are compatible
			if typesMatch(fieldType, sourceStruct.Name) {
				matched++
				fmt.Printf("      ✓ %s: %s matches source struct\n", field.Name, field.TypeName)
			} else {
				missing++
				fmt.Printf("      ✗ %s: type mismatch (field: %s, source: %s)\n",
					field.Name, fieldType, sourceStruct.Name)
			}
		} else {
			// Check if this looks like a struct type (has package prefix or is capitalized)
			// vs a primitive type (int32, string, etc.)
			if isStructType(fieldType) {
				missing++
				fmt.Printf("      ⚠ %s: struct not found in source (expected: %s)\n", field.Name, fieldType)
			} else {
				extra++
				fmt.Printf("      ℹ %s: %s (extra field)\n", field.Name, field.TypeName)
			}
		}
	}

	if matched > 0 || missing > 0 || extra > 0 {
		fmt.Printf("      Summary: %d matched, %d missing, %d extra\n", matched, missing, extra)
	}
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
