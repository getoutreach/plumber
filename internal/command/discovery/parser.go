// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: YAML parser for plumber discovery configuration
// Managed: true

package discovery

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ParseConfig parses a YAML configuration file
func ParseConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", path, err)
	}

	return ParseConfigBytes(data)
}

// ParseConfigBytes parses configuration from YAML bytes
func ParseConfigBytes(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// Validate the configuration
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// validateConfig validates the parsed configuration
func validateConfig(cfg *Config) error {
	if len(cfg.Applications) == 0 {
		return fmt.Errorf("no applications defined")
	}

	for i, app := range cfg.Applications {
		if app.Name == "" {
			return fmt.Errorf("application %d: name is required", i)
		}

		if len(app.Containers) == 0 {
			return fmt.Errorf("application %q: no containers defined", app.Name)
		}

		for j, container := range app.Containers {
			if err := validateContainer(&container, fmt.Sprintf("application %q container %d", app.Name, j)); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateContainer(container *Container, prefix string) error {
	if container.PlumberContainer == nil {
		return fmt.Errorf("%s: no plumber.container configuration", prefix)
	}

	cfg := container.PlumberContainer

	if cfg.Name == "" {
		return fmt.Errorf("%s: name is required", prefix)
	}

	if cfg.Container.Path == "" {
		return fmt.Errorf("%s: container.path is required", prefix)
	}

	return nil
}

// ContainerLookup returns the dependency container name configured for a
// directory (e.g. structure path dependency_container), or false when none is
// defined.
type ContainerLookup func(dir string) (string, bool)

// ProcessLoops processes loop configurations and expands containers
func ProcessLoops(cfg *Config, baseDir string) error {
	return ProcessLoopsWithContainers(cfg, baseDir, nil)
}

// ProcessLoopsWithContainers processes loop configurations and expands
// containers. When lookup returns a dependency container for a matched
// directory, it is exposed to the container templates as the "container"
// variable (container, container_slug, container_path) instead of the first
// captured group. Loop matches of the same plumber.container entry that hydrate
// to the same container name and path are merged into a single container with
// multiple sources.
func ProcessLoopsWithContainers(cfg *Config, baseDir string, lookup ContainerLookup) error {
	for appIdx := range cfg.Applications {
		app := &cfg.Applications[appIdx]
		var expandedContainers []Container

		for _, container := range app.Containers {
			if container.PlumberContainer == nil {
				expandedContainers = append(expandedContainers, container)
				continue
			}

			plumberCfg := container.PlumberContainer

			// If there's a loop configuration, expand it
			if container.Loop != nil {
				loopBaseDir := container.Loop.BaseDir
				paths := []string{container.Loop.Path}
				paths = append(paths, container.Loop.Paths...)
				var loopContainers []Container
				for _, p := range paths {
					expanded, err := processLoopPaths(plumberCfg, baseDir, loopBaseDir, app, p, lookup)
					if err != nil {
						return err
					}
					loopContainers = append(loopContainers, expanded...)
				}
				expandedContainers = append(expandedContainers, mergeContainers(loopContainers)...)
			} else {
				expandedContainers = append(expandedContainers, container)
			}
		}

		app.Containers = expandedContainers
	}

	return nil
}

// mergeContainers merges hydrated containers sharing the same name and
// container path, keeping the first occurrence and appending the sources of the
// others as AdditionalSources.
func mergeContainers(containers []Container) []Container {
	var (
		merged []Container
		index  = map[string]int{}
	)
	for _, c := range containers {
		cfg := c.PlumberContainer
		key := cfg.Name + "\x00" + cfg.Container.Path
		i, exists := index[key]
		if !exists {
			index[key] = len(merged)
			merged = append(merged, c)
			continue
		}
		target := merged[i].PlumberContainer
		for _, src := range cfg.Sources() {
			if !containsSource(target.Sources(), src) {
				target.AdditionalSources = append(target.AdditionalSources, src)
			}
		}
	}
	return merged
}

// containsSource reports whether sources already contain src.
func containsSource(sources []SourcePathConfig, src SourcePathConfig) bool {
	for _, s := range sources {
		if s.Path == src.Path {
			return true
		}
	}
	return false
}

func processLoopPaths(
	plumberCfg *PlumberContainerConfig, baseDir, loopBaseDir string, app *Application, path string, lookup ContainerLookup,
) ([]Container, error) {
	expandedContainers := []Container{}
	if path == "" {
		return expandedContainers, nil
	}
	iterator, err := NewPathIterator(baseDir, loopBaseDir, path)
	if err != nil {
		return nil, fmt.Errorf("application %q: failed to create path iterator: %w", app.Name, err)
	}

	matches, err := iterator.Iterate()

	if err != nil {
		return nil, fmt.Errorf("application %q: failed to iterate paths: %w", app.Name, err)
	}

	if len(matches) == 0 {
		return nil, nil
	}

	if lookup != nil {
		for i := range matches {
			if name, ok := lookup(matches[i].Path); ok {
				matches[i].Container = name
			}
		}
	}

	// Create hydrated configs
	hydrated := HydrateConfig(plumberCfg, matches)

	// Add each hydrated config as a separate container
	for _, h := range hydrated {
		newContainer := Container{
			PlumberContainer: h,
		}
		expandedContainers = append(expandedContainers, newContainer)
	}
	return expandedContainers, nil
}
