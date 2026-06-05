// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the handler notification registry and command execution
// for the shape command's plumber:notify system.

// Package handler provides the notification registry and command execution logic for the
// shape command's handler system. Handlers are command templates configured in YAML that
// are triggered by plumber:notify annotations on transformers. Named arguments from all
// notifications targeting the same handler are aggregated and made available to the
// command template for expansion.
package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"sync"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/render"
)

// Notification represents a single plumber:notify annotation that was processed
// during transformer execution.
type Notification struct {
	// Handler is the name of the target handler (first positional arg of plumber:notify).
	Handler string
	// NamedArgs contains the named arguments from the annotation (e.g. path="xy").
	NamedArgs map[string]string
}

// Registry collects notifications emitted by plumber:notify annotations and executes
// the corresponding handler commands after all transformations complete. It implements
// the contract.NotificationCollector interface.
type Registry struct {
	mu            sync.Mutex
	handlers      []config.HandlerConfig
	notifications []Notification
}

// NewRegistry creates a new handler registry with the given handler configurations.
func NewRegistry(handlers []config.HandlerConfig) *Registry {
	return &Registry{
		handlers: handlers,
	}
}

// Notify records a notification for the given handler name with the provided named arguments.
// It is safe for concurrent use.
func (r *Registry) Notify(handlerName string, namedArgs map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notifications = append(r.notifications, Notification{
		Handler:   handlerName,
		NamedArgs: namedArgs,
	})
}

// handlerTemplateData is the template context passed to handler command templates.
type handlerTemplateData struct {
	Source *handlerSourceData
}

// handlerSourceData exposes the aggregated named arguments from all notifications
// targeting a handler, with each key mapping to a slice of all collected values.
type handlerSourceData struct {
	NamedArgs map[string]string
}

// Execute runs all handlers that received notifications. For each configured handler
// with matching notifications, it aggregates named arguments, expands the command
// template, and executes the result via sh -c. Reporter events are emitted for each
// stage of the lifecycle. Returns the first error encountered.
func (r *Registry) Execute(ctx *contract.ShapingContext) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.notifications) == 0 {
		return nil
	}

	// Group notifications by handler name
	byHandler := make(map[string][]Notification)
	for _, n := range r.notifications {
		fmt.Println("Received:", n.Handler)
		byHandler[n.Handler] = append(byHandler[n.Handler], n)
	}

	for _, hc := range r.handlers {
		if hc.PlumberHandler == nil {
			continue
		}
		h := hc.PlumberHandler
		notifications, ok := byHandler[h.Name]
		if !ok {
			// no notifications for this handler, skip
			continue
		}

		seen := make(map[string]struct{})

		for _, n := range notifications {
			b, err := json.Marshal(n.NamedArgs)
			if err != nil {
				return fmt.Errorf("failed to marshal named args for handler %q: %w", h.Name, err)
			}

			// Skip duplicate notifications with the same named args
			if _, exists := seen[string(b)]; exists {
				continue
			}
			seen[string(b)] = struct{}{}

			// Expand command template
			expanded, err := expandCommand(h.Command, n.NamedArgs)
			if err != nil {
				ctx.HandlerError(h.Name, h.Command, fmt.Errorf("failed to expand handler command template: %w", err))
				return fmt.Errorf("handler %q: failed to expand command template: %w", h.Name, err)
			}

			ctx.HandlerExecuting(h.Name, expanded, n.NamedArgs)

			// Execute via sh -c
			cmd := exec.CommandContext(ctx, "sh", "-c", expanded)
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			if err := cmd.Run(); err != nil {
				output := stderr.String()
				if output == "" {
					output = stdout.String()
				}
				execErr := fmt.Errorf("handler %q: command failed: %w\noutput: %s", h.Name, err, output)
				ctx.HandlerError(h.Name, expanded, execErr)
				return execErr
			}

			ctx.HandlerCompleted(h.Name, expanded)
		}
	}

	return nil
}

// expandCommand expands the handler command template with the aggregated named arguments.
func expandCommand(commandTemplate string, namedArgs map[string]string) (string, error) {
	data := handlerTemplateData{
		Source: &handlerSourceData{
			NamedArgs: namedArgs,
		},
	}

	tmpl, err := template.New("handler-command").
		Option("missingkey=error").
		Funcs(render.GenericFunctions()).
		Funcs(sprig.TxtFuncMap()).
		Parse(commandTemplate)
	if err != nil {
		return "", fmt.Errorf("parsing command template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("executing command template: %w", err)
	}

	return buf.String(), nil
}
