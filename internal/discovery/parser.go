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

// ProcessLoops processes loop configurations and expands containers
func ProcessLoops(cfg *Config, baseDir string) error {
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
			iterator, err := NewPathIterator(baseDir, container.Loop.Path)
			if err != nil {
				return fmt.Errorf("application %q: failed to create path iterator: %w", app.Name, err)
			}

			matches, err := iterator.Iterate()

			if err != nil {
				return fmt.Errorf("application %q: failed to iterate paths: %w", app.Name, err)
			}

			if len(matches) == 0 {
				return fmt.Errorf("application %q: no paths matched pattern %q", app.Name, container.Loop.Path)
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
			} else {
				expandedContainers = append(expandedContainers, container)
			}
		}

		app.Containers = expandedContainers
	}

	return nil
}
