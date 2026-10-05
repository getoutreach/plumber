// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements inplace merging of generated constant declarations
// into existing Go source files. Constants are added if missing and skipped if any of the
// declared names already exist. Grouped const blocks (including iota-based) are preserved
// as a single GenDecl so that the iota numbering stays valid.

package shape

import (
	"go/token"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// mergeConst merges a generated constant declaration into the existing package.
// The whole GenDecl is treated as an atomic unit so grouped const blocks (e.g.
// `const ( A = iota; B; C )`) preserve their iota numbering after merge.
//
// Dedup semantics: if ANY name declared inside the GenDecl already exists in
// pkg.Consts, the entire group is skipped. Otherwise the GenDecl is cloned (with
// its Lparen/Rparen and group doc comment preserved) and appended to the file
// selected by output (creating that file when necessary).
func mergeConst(
	pkg *model.Package, generated *dst.GenDecl, output string, importMap map[string]string,
) (*dst.File, error) {
	// Collect every name declared across all ValueSpecs in the group.
	var names []string
	for _, spec := range generated.Specs {
		vs, ok := spec.(*dst.ValueSpec)
		if !ok {
			continue
		}
		for _, ident := range vs.Names {
			if ident != nil && ident.Name != "" {
				names = append(names, ident.Name)
			}
		}
	}
	if len(names) == 0 {
		return nil, nil
	}

	// Skip the whole group if any of the declared names already exists. This
	// keeps grouped const blocks atomic, which is required for iota-based
	// declarations to render correctly.
	for _, name := range names {
		if _, found := lo.Find(pkg.Consts, func(c *model.PackageConst) bool {
			return c.Name == name
		}); found {
			return nil, nil
		}
	}

	// Pick the destination file. Prefer the file dictated by the output
	// annotation (creating it if needed); fall back to the first existing file
	// in the package so legacy callers without an output configured still work.
	targetFile := findOrCreateOutputFile(pkg, output)
	if targetFile == nil && len(pkg.Package.Syntax) > 0 {
		targetFile = pkg.Package.Syntax[0]
	}
	if targetFile == nil {
		return nil, nil
	}

	// Rewrite type and value expressions on each spec so package-qualified
	// identifiers carry the canonical Ident.Path form expected by the printer.
	for _, spec := range generated.Specs {
		vs, ok := spec.(*dst.ValueSpec)
		if !ok {
			continue
		}
		if vs.Type != nil {
			vs.Type = astx.RewriteExpr(vs.Type, importMap)
		}
		for i, v := range vs.Values {
			vs.Values[i] = astx.RewriteExpr(v, importMap)
		}
	}

	// Build a new GenDecl preserving the grouping (Lparen/Rparen) and the
	// original spec order. Copy the group-level doc decorations from the
	// generated decl; per-spec docs are preserved because we reuse the
	// original Spec values.
	newDecl := &dst.GenDecl{
		Tok:    token.CONST,
		Lparen: generated.Lparen,
		Rparen: generated.Rparen,
		Specs:  generated.Specs,
	}
	newDecl.Decs.Start = append(newDecl.Decs.Start, generated.Decs.Start...)
	newDecl.Decs.Before = dst.EmptyLine
	newDecl.Decs.After = dst.EmptyLine

	targetFile.Decls = append(targetFile.Decls, newDecl)
	return targetFile, nil
}
