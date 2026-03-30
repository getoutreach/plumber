package inspect

import (
	"encoding/json"
	"fmt"

	"github.com/getoutreach/plumber/internal/astx/inspect"
	"github.com/getoutreach/plumber/query/model"
	"gopkg.in/yaml.v3"
)

func Run(config *InspectConfig, args []string) error {
	filenames, err := inspect.ScanFiles("./", args)
	if err != nil {
		return fmt.Errorf("failed to scan files: %w", err)
	}
	pkgs, err := inspect.Inspect(filenames)
	if err != nil {
		return fmt.Errorf("failed to inspect files: %w", err)
	}

	return format(config, pkgs)
}

func format(config *InspectConfig, pkgs []*model.Package) error {
	switch config.Format {
	case "json":
		return formatJSON(pkgs)
	case "yaml":
		return formatYAML(pkgs)
	default:
		return fmt.Errorf("unsupported format: %s", config.Format)
	}
}

func formatJSON(pkgs []*model.Package) error {
	data, err := json.Marshal(pkgs)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	fmt.Println(string(data))
	return nil
}

func formatYAML(pkgs []*model.Package) error {
	data, err := yaml.Marshal(pkgs)
	if err != nil {
		return fmt.Errorf("failed to marshal YAML: %w", err)
	}
	fmt.Println(string(data))
	return nil
}
