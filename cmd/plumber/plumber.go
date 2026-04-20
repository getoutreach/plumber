// Copyright 2026 Outreach Corporation. Licensed under the Apache License 2.0.

// Description: This file is the entrypoint for the plumber CLI
// command for plumber.
// Managed: true

package main

import (
	"context"

	oapp "github.com/getoutreach/gobox/pkg/app"
	"github.com/getoutreach/gobox/pkg/cfg"
	gcli "github.com/getoutreach/gobox/pkg/cli"
	"github.com/getoutreach/plumber/cmd/plumber/discovery"
	"github.com/getoutreach/plumber/cmd/plumber/inspect"
	"github.com/getoutreach/plumber/cmd/plumber/shape"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	// Place any extra imports for your startup code here
	// <<Stencil::Block(imports)>>
	// <</Stencil::Block>>
)

// HoneycombTracingKey gets set by the Makefile at compile-time which is pulled
// down by devconfig.sh.
var HoneycombTracingKey = "NOTSET" //nolint:gochecknoglobals // Why: We can't compile in things as a const.

// TeleforkAPIKey gets set by the Makefile at compile-time which is pulled
// down by devconfig.sh.
var TeleforkAPIKey = "NOTSET" //nolint:gochecknoglobals // Why: We can't compile in things as a const.

// <<Stencil::Block(honeycombDataset)>>

// HoneycombDataset is a constant denoting the dataset that traces should be stored
// in in honeycomb.
const HoneycombDataset = ""

// <</Stencil::Block>>

// <<Stencil::Block(global)>>

// <</Stencil::Block>>

// main is the entrypoint for the plumber CLI.
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	log := logrus.New()

	// <<Stencil::Block(init)>>

	// <</Stencil::Block>>

	app := cli.App{
		Version: oapp.Version,
		Name:    "plumber",
		// <<Stencil::Block(app)>>

		// <</Stencil::Block>>
	}
	app.Flags = []cli.Flag{
		// <<Stencil::Block(flags)>>

		// <</Stencil::Block>>
	}
	app.Commands = []*cli.Command{
		// <<Stencil::Block(commands)>>
		{
			Name:   "discovery",
			Usage:  "Run autodiscovery for plumber containers",
			Action: discovery.Run,
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     "config",
					Aliases:  []string{"c"},
					Usage:    "Path to plumber.yaml configuration file",
					Required: true,
				},
			},
		},
		{
			Name:   "shape",
			Usage:  "Run shape command",
			Action: shape.Run,
			Flags: []cli.Flag{

				&cli.StringFlag{
					Name:    "config",
					Aliases: []string{"c"},
					Usage:   "Path to plumber.yaml configuration file",
				},
				&cli.StringFlag{
					Name:  "type",
					Usage: "FQN of the target type (single-type mode, requires --macro)",
				},
				&cli.StringFlag{
					Name:  "macro",
					Usage: "Macro name to apply in single-type mode (requires --type)",
				},
				&cli.StringSliceFlag{
					Name:  "macro-arg",
					Usage: "Positional arg for the macro (repeatable, requires --macro)",
				},
				&cli.StringSliceFlag{
					Name:  "macro-named-arg",
					Usage: "Named arg as key=value for the macro (repeatable, requires --macro)",
				},
			},
		},
		{
			Name:   "inspect",
			Usage:  "Run inspect command",
			Action: inspect.Run,
			Flags: []cli.Flag{

				&cli.StringFlag{
					Name:    "config",
					Aliases: []string{"c"},
					Usage:   "Path to plumber.yaml configuration file",
				},
				&cli.StringFlag{
					Name:    "format",
					Aliases: []string{"f"},
					Value:   "",
					Usage:   "Output format for inspect command (json or yaml)",
				},
			},
		},
		// <</Stencil::Block>>
	}

	// <<Stencil::Block(postApp)>>

	// <</Stencil::Block>>

	// Insert global flags, tracing, updating and start the application.
	gcli.Run(ctx, cancel, &app, &gcli.Config{
		Logger: log,
		Telemetry: gcli.TelemetryConfig{
			Otel: gcli.TelemetryOtelConfig{
				Dataset:         HoneycombDataset,
				HoneycombAPIKey: cfg.SecretData(HoneycombTracingKey),
			},
		},
	})
}
