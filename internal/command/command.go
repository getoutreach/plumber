package command

import (
	"fmt"
	"os"

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
