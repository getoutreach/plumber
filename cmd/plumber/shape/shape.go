package shape

import (
	"fmt"
	"path/filepath"

	"github.com/getoutreach/plumber/internal/command"
	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/urfave/cli/v2"
)

// Run executes the discovery command
func Run(c *cli.Context) error {
	args := c.Args().Slice()

	configPath := c.String("config")

	shapeConfig := shape.ShapeConfig{}

	if configPath != "" {

		// Resolve absolute path for config file
		absConfigPath, err := filepath.Abs(configPath)
		if err != nil {
			return fmt.Errorf("failed to resolve config path: %w", err)
		}

		// Use config file's directory as base directory
		// baseDir := filepath.Dir(absConfigPath)

		// Parse the configuration
		cfg, err := command.ParseConfig[shape.Config](absConfigPath)
		if err != nil {
			return fmt.Errorf("failed to parse config: %w", err)
		}
		shapeConfig = cfg.Shape
	}

	err := shape.Run(&shapeConfig, args)
	if err != nil {
		return fmt.Errorf("failed to run shape command: %w", err)
	}

	return nil
}
