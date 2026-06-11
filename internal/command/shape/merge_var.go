// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements inplace merging of generated variable declarations
// into existing Go source files. Variables are added if missing and skipped if already present.

package shape

import (
	"go/token"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// mergeVar merges a generated variable declaration into the existing package.
// If the variable does not exist (by name), it is added to the file selected by output
// (creating that file when necessary). If it already exists, it is skipped.
func mergeVar(
	pkg *model.Package, generated *dst.ValueSpec, output string, importMap map[string]string,
) (*dst.File, error) {
	if len(generated.Names) == 0 {
		return nil, nil
	}

	varName := generated.Names[0].Name

	// Check if variable already exists in the package.
	_, found := lo.Find(pkg.Vars, func(v *model.PackageVar) bool {
		return v.Name == varName
	})
	if found {
		// Variable already exists — skip.
		return nil, nil
	}

	// Variable not found — pick the destination file. Prefer the file dictated by
	// the output annotation (creating it if needed); fall back to the first existing
	// file in the package so legacy callers without an output configured still work.
	targetFile := findOrCreateOutputFile(pkg, output)
	if targetFile == nil && len(pkg.Package.Syntax) > 0 {
		targetFile = pkg.Package.Syntax[0]
	}
	if targetFile == nil {
		return nil, nil
	}

	// Annotate type expression for proper import resolution
	if generated.Type != nil {
		generated.Type = astx.RewriteExpr(generated.Type, importMap)
	}
	for i, v := range generated.Values {
		generated.Values[i] = astx.RewriteExpr(v, importMap)
	}

	// Create a GenDecl to wrap the ValueSpec
	newDecl := &dst.GenDecl{
		Tok:   token.VAR,
		Specs: []dst.Spec{generated},
	}
	newDecl.Decs.Before = dst.EmptyLine
	newDecl.Decs.After = dst.EmptyLine

	targetFile.Decls = append(targetFile.Decls, newDecl)

	return targetFile, nil
}
