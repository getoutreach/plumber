// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains cross-command types and functions used in the rendering process

// Package render provides utilities for rendering templates for container files in the discovery command.
package render

import (
	"embed"
	"fmt"
	"io"
	"path"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/command/shape/render/view"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/query/model"
)

// PlaceholderName is the name used in templates to reference the context for rendering.
const PlaceholderName = "plumber"

// Mode constants for transformers
const (
	// ModeInPlace is the mode for transformers that indicates the generated code should be merged
	// in place with the existing source file.
	ModeInPlace = "inplace"

	// ModeGenerated is the mode for transformers that indicates the generated code should be written
	// to a separate file.
	ModeGenerated = "generated"
)

// Vars
var (
	// EmbededTemplates is an embedded filesystem containing the default templates for rendering Go code.
	//
	//go:embed templates/*
	EmbededTemplates embed.FS
)

// DstOutput represents the output of rendering a transformation, containing the generated dst.File and its
// associated package information.
type DstOutput struct {
	File    *dst.File
	Package *decorator.Package
}

// Output represents the result of rendering a transformation, containing the output filename, rendered content,
// and any associated modules or DST output.
type Output struct {
	Filename string
	Modules  *ModuleRegister
	Package  *model.Package
	Content  []byte
	Dst      *DstOutput
}

func DefaultScope(context Context, scope Scope, output string) Scope {
	scope["Package"] = Scope{
		"Name": path.Base(path.Dir(output)),
		"Path": context.GetPkgPath(),
	}
	scope["Output"] = Scope{
		"Path": output,
	}
	return scope
}

func WithTemplates(templates ...string) gen.RenderOptionsFunc {
	return gen.WithFS(EmbededTemplates,
		templates...,
	)
}

func WithBaseTemplates(extra ...string) gen.RenderOptionsFunc {
	return gen.WithFS(EmbededTemplates,
		append([]string{
			"templates/file.gtpl",
		}, extra...)...,
	)
}

func Render(
	context Context, template string, scope Scope, output string, opener gen.FileOpener, opts ...gen.WriterOption,
) (*Output, error) {
	writer := gen.NewWriter(append([]gen.WriterOption{gen.WithFileOpener(opener), func(wc *gen.WriterConfig) {
		wc.Overwrite = true
		wc.WriterOptions = append(wc.WriterOptions, func(s *gen.BlockWriterSettings) {
			s.PlaceholderName = PlaceholderName
		})
	}}, opts...)...)
	ctx := gen.NewContext("neco")

	scope["Modules"] = context.GetModules()
	if scope["File"] == nil {
		scope["File"] = Scope{}
	}

	scope = DefaultScope(context, scope, output)

	fm, dispose := WithRenderFuncMap(context, scope, output)
	defer dispose()

	features := gen.Features{
		gen.FeatureFunc(func(ctx *gen.Context, wr *gen.Writer) error {
			c := view.Base{
				Scope: scope,
			}
			return ctx.Write(wr, output, func(ctx *gen.Context, w io.Writer) error {
				return gen.RenderContent(ctx, template, w, c,
					fm,
					gen.WithRenderOptions(context.GetRenderOptions()...),
				)
			})
		}),
	}

	if err := features.Render(ctx, writer); err != nil {
		return nil, fmt.Errorf("Error during rendering: %w", err)
	}

	return &Output{
		Filename: output,
		Modules:  context.GetModules(),
		Content:  opener.Content(output),
	}, nil
}

func File(
	context Context, entryTemplate string, scope Scope, output string,
) (*Output, error) {
	o, err := Render(context, entryTemplate, scope, output, gen.NewReadOnlyFileOpener())
	if err != nil {
		return nil, fmt.Errorf("error during rendering: %w", err)
	}

	o, err = Finalize(context, scope, []string{string(o.Content)}, output, gen.NewSystemFileOpener())
	if err != nil {
		return nil, fmt.Errorf("error during finalization: %w", err)
	}
	return o, nil
}
