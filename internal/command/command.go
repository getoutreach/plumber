// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements YAML configuration parsing helpers used across plumber CLI commands.

// Package command provides shared utilities for parsing YAML configuration files used by plumber CLI commands.
package command

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ParseConfigBytes parses configuration from YAML bytes
func ParseConfigBytes[T any](data []byte) (*T, error) {
	var cfg T
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	return &cfg, nil
}

// ParseConfig parses a YAML configuration file
func ParseConfig[T any](path string) (*T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", path, err)
	}

	return ParseConfigBytes[T](data)
}

// ParseConfig parses a YAML configuration file
func ParseConfigs[T any](paths ...string) ([]*T, error) {
	var configs []*T
	for _, path := range paths {
		matches, err := filepath.Glob(path)
		if err != nil {
			return nil, fmt.Errorf("failed to glob path %q: %w", path, err)
		}
		for _, match := range matches {
			// TODO(pavelsm): use reporter fmt.Println("Loading", match)
			data, err := os.ReadFile(match)
			if err != nil {
				return nil, fmt.Errorf("failed to read config file %q: %w", match, err)
			}

			cfg, err := ParseConfigBytes[T](data)
			if err != nil {
				return nil, fmt.Errorf("failed to parse config file %q: %w", match, err)
			}
			configs = append(configs, cfg)
		}
	}
	return configs, nil
}
