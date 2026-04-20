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
// If the variable does not exist (by name), it is added. If it exists, it is skipped.
func mergeVar(pkg *model.Package, generated *dst.ValueSpec, importMap map[string]string) (*dst.File, error) {
	if len(generated.Names) == 0 {
		return nil, nil
	}

	varName := generated.Names[0].Name

	// Check if variable already exists in the package
	_, found := lo.Find(pkg.Vars, func(v *model.PackageVar) bool {
		return v.Name == varName
	})
	if found {
		// Variable already exists — skip
		return nil, nil
	}

	// Variable not found — add it to the first file in the package
	var targetFile *dst.File
	if len(pkg.Package.Syntax) > 0 {
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
