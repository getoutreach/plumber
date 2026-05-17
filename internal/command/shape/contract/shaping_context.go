// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines shaping context functions
package contract

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/getoutreach/plumber/query/model"
)

func NewShapingContext(
	ctx context.Context,
	reporter Reporter,
	templateLoader TemplateLoader,
	structurePathResolver StructurePathResolver,
) *ShapingContext {
	return &ShapingContext{
		Context:               ctx,
		Reporter:              reporter,
		TemplateLoader:        templateLoader,
		StructurePathResolver: structurePathResolver,
	}
}

func (c *ShapingContext) Tap(fn func(*ShapingContext)) *ShapingContext {
	fn(c)
	return c
}

func (c *ShapingContext) TransformerAdded(transformer Transformer, node model.Node) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:        EventTransformerAdded,
			Transformer: transformer,
			Node:        node,
		})
	}
}

func (c *ShapingContext) TransformerSkipped(transformer Transformer, node model.Node, message string) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:        EventTransformerSkipped,
			Transformer: transformer,
			Node:        node,
			Message:     message,
		})
	}
}

func (c *ShapingContext) TransformerError(transformer Transformer, node model.Node, err error) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:        EventTransformerError,
			Transformer: transformer,
			Node:        node,
			Error:       err,
		})
	}
}

func (c *ShapingContext) TransformerOutput(transformer Transformer, filename string) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:        EventTransformerOutput,
			Transformer: transformer,
			Path:        filename,
		})
	}
}

func (c *ShapingContext) TransformerInfo(transformer Transformer, message string) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:        EventTransformerInfo,
			Transformer: transformer,
			Message:     message,
		})
	}
}

func (c *ShapingContext) RestoredOutput(filename string, err error) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:  EventTransformerRestored,
			Path:  filename,
			Error: err,
		})
	}
}

func (c *ShapingContext) QueryExecuted(query, filename string) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:    EventQueryExecuted,
			Message: query,
			Path:    filename,
		})
	}
}

func (c *ShapingContext) QueryError(query, filename string, err error) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:    EventQueryError,
			Message: query,
			Path:    filename,
			Error:   err,
		})
	}
}

func (c *ShapingContext) HandlerTriggered(handlerName string, transformer Transformer, node model.Node) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:        EventHandlerTriggered,
			Message:     handlerName,
			Transformer: transformer,
			Node:        node,
		})
	}
}

func (c *ShapingContext) HandlerExecuting(handlerName, command string) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:    EventHandlerExecuting,
			Message: handlerName,
			Path:    command,
		})
	}
}

func (c *ShapingContext) HandlerCompleted(handlerName, command string) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:    EventHandlerCompleted,
			Message: handlerName,
			Path:    command,
		})
	}
}

func (c *ShapingContext) HandlerError(handlerName, command string, err error) {
	if c.Reporter != nil {
		c.Reporter.Notify(ReporterEvent{
			Kind:    EventHandlerError,
			Message: handlerName,
			Path:    command,
			Error:   err,
		})
	}
}

func (c *ShapingContext) DeriveModulePath(dir string) (string, error) {
	if dir == "." {
		return c.Module.Path, nil
	}

	if strings.HasPrefix(dir, c.Module.Dir) {
		dir = strings.TrimPrefix(dir, c.Module.Dir)
	} else {
		return "", fmt.Errorf("output directory %q is outside of the module directory %q", dir, c.Module.Dir)
	}
	return path.Join(c.Module.Path, dir), nil
}
