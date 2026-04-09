// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the Logger type wrapping pterm for structured, leveled log output during code generation.

package gen

import (
	"fmt"

	"github.com/pterm/pterm"
)

type Logger struct {
	instance *pterm.Logger
}

type MessageLogger func(msg string, args ...[]pterm.LoggerArgument)

func NewLogger() *Logger {
	logger := pterm.DefaultLogger.WithLevel(pterm.LogLevelTrace)

	// Define a new style for the "priority" key.
	priorityStyle := map[string]pterm.Style{
		"template": *pterm.NewStyle(pterm.FgGray),
	}

	// Overwrite all key styles with the new map.
	logger = logger.WithKeyStyles(priorityStyle)

	return &Logger{
		instance: logger,
	}
}

func (l *Logger) GenerationReport(ctx *Context, filename string, err error) {
	var (
		logger     = l.instance.Info
		loggerArgs []pterm.LoggerArgument
	)
	if err != nil {
		logger = l.instance.Error
	}
	if len(ctx.warn) > 0 {
		logger = l.instance.Warn
	}
	if err != nil || len(ctx.warn) > 0 || len(ctx.errors) > 0 {
		args := ctx.metadata
		if err != nil {
			args["error"] = err
		}
		if len(ctx.warn) > 0 {
			for i, w := range ctx.warn {
				args[fmt.Sprintf("warn(%d)", i)] = w.message
			}
		}
		if len(ctx.errors) > 0 {
			for i, w := range ctx.errors {
				args[fmt.Sprintf("error(%d)", i)] = w.message
			}
		}
		loggerArgs = l.instance.ArgsFromMap(args)
	}
	logger(filename, loggerArgs)
}
