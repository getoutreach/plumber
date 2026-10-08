// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements inplace merging of generated variable declarations
// into existing Go source files. Variables (exported or unexported, including those with
// generic types and initializers) are added if missing. When a variable already exists the
// existing declaration is the source of truth: its type is never rewritten, its doc comment
// is reconciled using the preserve-manual rule, and its initializer is blended with the
// generated one (missing call arguments / literal entries are inserted, existing ones kept).

package shape

import (
	"go/token"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// mergeVar merges a generated variable ValueSpec (declared inside genDecl) into the
// existing package.
//
// Existence is decided by name: when ANY name declared by the spec already exists as a
// package-level variable, the spec is not added (multi-name specs such as `var a, b = f()`
// are kept atomic). Existing variables are looked up both in pkg.Vars and directly in the
// package syntax so that unexported variables — which the inspect pipeline does not record
// in pkg.Vars — are detected as well. On collision the existing variable is blended with
// the generated one (see mergeExistingVar) and the containing file is returned when
// anything changed; otherwise nil is returned.
//
// When the variable does not exist it is appended to the file selected by output (creating
// that file when necessary) as a standalone `var` declaration carrying the generated doc.
func mergeVar(
	pkg *model.Package, genDecl *dst.GenDecl, generated *dst.ValueSpec, output string, importMap map[string]string,
) (*dst.File, error) {
	names := valueSpecNames(generated)
	if len(names) == 0 {
		return nil, nil
	}

	// The doc of a standalone `var x T` lives on the GenDecl; inside a grouped
	// `var ( ... )` block each spec carries its own doc.
	genDoc := generated.Decs.Start
	if genDecl != nil && !genDecl.Lparen {
		genDoc = genDecl.Decs.Start
	}

	for _, name := range names {
		if match := findTopLevelValueSpec(pkg, token.VAR, name); match != nil {
			return mergeExistingVar(match, generated, genDoc, importMap), nil
		}
	}

	// Defensive fallback: pkg.Vars may know about a variable whose syntax we could not
	// locate (e.g. a file outside pkg.Package.Syntax). Never duplicate it.
	for _, name := range names {
		if _, found := lo.Find(pkg.Vars, func(v *model.PackageVar) bool {
			return v.Name == name
		}); found {
			return nil, nil
		}
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

	// Rewrite type and value expressions (including generic instantiations such as
	// `pkg.Schema[a.T, b.U]` and their initializers) so package-qualified identifiers
	// carry the canonical Ident.Path form expected by the restorer.
	if generated.Type != nil {
		generated.Type = astx.RewriteExpr(generated.Type, importMap)
	}
	for i, v := range generated.Values {
		generated.Values[i] = astx.RewriteExpr(v, importMap)
	}

	// Wrap the spec in a standalone GenDecl. The doc is moved from the spec (grouped
	// source) or copied from the source GenDecl onto the new GenDecl.
	newDecl := &dst.GenDecl{
		Tok:   token.VAR,
		Specs: []dst.Spec{generated},
	}
	newDecl.Decs.Start = append(newDecl.Decs.Start, genDoc...)
	if genDecl != nil && genDecl.Lparen {
		generated.Decs.Start = nil
	}
	generated.Decs.Before = dst.NewLine
	generated.Decs.After = dst.NewLine
	newDecl.Decs.Before = dst.EmptyLine
	newDecl.Decs.After = dst.EmptyLine

	targetFile.Decls = append(targetFile.Decls, newDecl)

	return targetFile, nil
}

// mergeExistingVar blends a generated variable into an existing declaration.
//
//   - The doc comment is reconciled with the preserve-manual rule. For standalone
//     declarations the doc lives on the GenDecl; for grouped blocks on the ValueSpec.
//   - The initializer is deep-merged when the existing spec declares the same names in
//     the same order: arguments of matching calls (e.g. `mapping.NewSchema(...)`) and
//     entries of matching composite literals that are missing in the existing code are
//     inserted, while existing arguments are kept as written. The declared type and any
//     initializer that targets a different function are never rewritten.
//
// Returns the containing file when anything changed, otherwise nil so untouched files
// are not rewritten.
func mergeExistingVar(
	match *valueSpecMatch, generated *dst.ValueSpec, genDoc dst.Decorations, importMap map[string]string,
) *dst.File {
	if match == nil {
		return nil
	}
	target := &match.Spec.Decs.Start
	if !match.Decl.Lparen {
		target = &match.Decl.Decs.Start
	}
	changed := mergeDocComment(target, genDoc)

	if sameNames(match.Spec, generated) && mergeValues(match.Spec.Values, generated.Values, importMap) {
		changed = true
	}

	if changed {
		return match.File
	}
	return nil
}

// sameNames reports whether two value specs declare exactly the same names in order,
// which makes pairwise merging of their initializers safe.
func sameNames(a, b *dst.ValueSpec) bool {
	an, bn := valueSpecNames(a), valueSpecNames(b)
	if len(an) != len(bn) || len(a.Names) != len(b.Names) {
		return false
	}
	for i := range an {
		if an[i] != bn[i] {
			return false
		}
	}
	return true
}
