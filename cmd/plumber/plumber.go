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
