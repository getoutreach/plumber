// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains commented-out lint/package description generation utilities, preserved for future reference.

package gen

// func PackageDescription(outputDir, p, description, packageDescription string) Features {
// 	writerConfig := WriterConfig{
// 		OutputDir: outputDir,
// 		Overwrite: true,
// 	}
// 	pkg := path.Base(path.Dir(p))

// 	if packageDescription == "" {
// 		packageDescription = "contains " + description
// 	}

// 	opts := []RenderOption{
// 		WithTemplateFunc(LoadBaseTemplate("templates/lint/*")),
// 		// WithFuncMap(GRPCFuncMap),
// 	}

// 	return Features{
// 		FeatureFunc(func(ctx *Context, wr *Writer) error {
// 			return ctx.Write(wr,
// 				p,
// 				&writerConfig, func(ctx *Context, w io.Writer) error {
// 					return RenderContent(ctx,
// 						"lint_package_description",
// 						w, map[string]string{
// 							"package":            pkg,
// 							"description":        description,
// 							"packageDescription": packageDescription,
// 						},
// 						opts...,
// 					)
// 				})
// 		}),
// 	}
// }
