package render

import (
	"fmt"
	"io"

	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/internal/render/view"
	"github.com/getoutreach/plumber/query/model"
)

func Shape(context Context, tp *model.Type, scope map[string]any, output string, opener gen.MemoryFileOpener) (string, error) {
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
				return gen.RenderContent(ctx, "plumber/command/shape", w, c,
					withRenderFuncMap(context),
					gen.WithFS(embededTemplates,
						"templates/command/command.gtpl",
						"templates/command/command_shape.gtpl",
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
