package render

import (
	"embed"
	"fmt"
	"html/template"
	"io"
	"path"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render/view"
	"github.com/getoutreach/plumber/query/model"
)

var (
	//go:embed templates/*
	embededTemplates embed.FS
)

type Context struct {
	Modules *ModuleRegister
	Ignores *Ignores
	PkgPath string
}

type DstOutput struct {
	File    *dst.File
	Package *decorator.Package
}

type Output struct {
	Filename string
	Modules  *ModuleRegister
	Content  []byte
	Dst      *DstOutput
}

func withRenderFuncMap(context Context) gen.RenderOptionsFunc {
	functions := template.FuncMap{
		"extend":           extend,
		"type":             typesRenderer(context.PkgPath, context.Modules),
		"annotation":       annotation,
		"annotation_value": annotationValue,
		"comment":          comment,
		"ignored":          ignored(context.Ignores),
		"filter_elements":  filterElements,
	}
	return gen.WithFuncMap(functions)
}

func Derive(context Context, tp *model.Type, scope map[string]any, output string, opener gen.MemoryFileOpener) (string, error) {
	writer := gen.NewWriter(gen.WithFileOpener(opener), func(wc *gen.WriterConfig) {
		wc.Overwrite = true
		wc.WriterOptions = append(wc.WriterOptions, func(s *gen.BlockWriterSettings) {
			s.PlaceholderName = "plumber"
		})
	})

	ctx := gen.NewContext("neco")

	scope = DefaultScope(scope, output)

	features := gen.Features{
		gen.FeatureFunc(func(ctx *gen.Context, wr *gen.Writer) error {
			c := &view.Struct{
				Type: tp,
				Base: view.Base{
					Scope: scope,
				},
			}
			return ctx.Write(wr, output, func(ctx *gen.Context, w io.Writer) error {
				return gen.RenderContent(ctx, "plumber/command/derive", w, c,
					withRenderFuncMap(context),
					gen.WithFS(embededTemplates,
						"templates/command/command_derive.gtpl",
					),
					gen.WithTemplateFunc(gen.LoadBaseTemplate(
						"templates/new.gtpl",
					)),
				)
			})
		}),
	}

	if err := features.Render(ctx, writer); err != nil {
		return "", fmt.Errorf("Error during rendering: %w", err)
	}

	content := opener.Content(output)

	return string(content), nil
}

func Finalize(context Context, scope map[string]any, parts []string, output string, opener gen.FileOpener, opts ...gen.WriterOption) (*Output, error) {
	writer := gen.NewWriter(append([]gen.WriterOption{gen.WithFileOpener(opener), func(wc *gen.WriterConfig) {
		wc.Overwrite = true
		wc.WriterOptions = append(wc.WriterOptions, func(s *gen.BlockWriterSettings) {
			s.PlaceholderName = "plumber"
		})
	}}, opts...)...)
	ctx := gen.NewContext("neco")

	scope["Content"] = strings.Join(parts, "\n")

	scope["Modules"] = context.Modules

	scope = DefaultScope(scope, output)

	features := gen.Features{
		gen.FeatureFunc(func(ctx *gen.Context, wr *gen.Writer) error {
			c := view.Base{
				Scope: scope,
			}
			return ctx.Write(wr, output, func(ctx *gen.Context, w io.Writer) error {
				return gen.RenderContent(ctx, "plumber/command/derive/file/content", w, c,
					withRenderFuncMap(context),
					gen.WithFS(embededTemplates,
						"templates/command/command_derive.gtpl",
					),
					gen.WithTemplateFunc(gen.LoadBaseTemplate(
						"templates/new.gtpl",
					)),
				)
			})
		}),
	}

	if err := features.Render(ctx, writer); err != nil {
		return nil, fmt.Errorf("Error during rendering: %w", err)
	}

	return &Output{
		Filename: output,
		Modules:  context.Modules,
		Content:  opener.Content(output),
	}, nil
}

func DefaultScope(scope map[string]any, output string) map[string]any {
	scope["Package"] = map[string]any{
		"Name": path.Base(path.Dir(output)),
	}

	return scope
}
