package inspect

import (
	"fmt"
	"path/filepath"

	"github.com/getoutreach/plumber/internal/command"
	"github.com/getoutreach/plumber/internal/command/inspect"
	"github.com/urfave/cli/v2"
)

// Run executes the discovery command
func Run(c *cli.Context) error {
	args := c.Args().Slice()

	configPath := c.String("config")
	format := c.String("format")

	cfg := &inspect.InspectConfig{}

	if configPath != "" {

		// Resolve absolute path for config file
		absConfigPath, err := filepath.Abs(configPath)
		if err != nil {
			return fmt.Errorf("failed to resolve config path: %w", err)
		}

		// Use config file's directory as base directory
		// baseDir := filepath.Dir(absConfigPath)

		// Parse the configuration
		c, err := command.ParseConfig[inspect.Config](absConfigPath)
		if err != nil {
			return fmt.Errorf("failed to parse config: %w", err)
		}
		cfg = c.Inspect
	}

	if format != "" {
		cfg.Format = format
	}
	if cfg.Format == "" {
		cfg.Format = "json" // default to json if not specified
	}

	err := inspect.Run(cfg, args)
	if err != nil {
		return fmt.Errorf("failed to run inspect command: %w", err)
	}

	return nil
}
