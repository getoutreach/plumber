// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides utilities for managing import declarations in dst.File AST structures.

package astx

import (
	"go/token"
	"path"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
)

// EnsureImport ensures an import is present in the file
func EnsureImport(pkg *decorator.Package, file *dst.File, importPath string) {
	// Check if import already exists
	for _, imp := range file.Imports {
		if strings.Trim(imp.Path.Value, `"`) == importPath {
			return
		}
	}

	// Add the import
	newImport := &dst.ImportSpec{
		Path: &dst.BasicLit{
			Kind:  token.STRING,
			Value: `"` + importPath + `"`,
		},
	}
	file.Imports = append(file.Imports, newImport)

	// Also add to declarations if needed
	var found bool
	for _, decl := range file.Decls {
		if genDecl, ok := decl.(*dst.GenDecl); ok && genDecl.Tok == token.IMPORT {
			genDecl.Specs = append(genDecl.Specs, newImport)
			found = true
			break
		}
	}

	// If no import declaration exists, create one
	if !found {
		newGenDecl := &dst.GenDecl{
			Tok:   token.IMPORT,
			Specs: []dst.Spec{newImport},
		}
		// Set the token to import
		newGenDecl.Decs.Before = dst.NewLine
		newGenDecl.Decs.After = dst.NewLine

		file.Decls = append(file.Decls, newGenDecl)
	}
}

// BuildImportMap builds a map from the local package name to the full import
// path for every import declared in file. For aliased imports the alias is
// used as the key; otherwise the last path segment is used.
func BuildImportMap(file *dst.File) map[string]string {
	m := make(map[string]string)
	for _, imp := range file.Imports {
		if imp.Path == nil {
			continue
		}
		fullPath := strings.Trim(imp.Path.Value, `"`)
		var localName string
		if imp.Name != nil && imp.Name.Name != "" && imp.Name.Name != "_" && imp.Name.Name != "." {
			localName = imp.Name.Name
		} else {
			localName = path.Base(fullPath)
		}
		m[localName] = fullPath
	}
	return m
}

// AnnotateFieldIdents rewrites the type expression of a field, replacing every
// *dst.SelectorExpr whose X is a package-qualifier ident with a single
// *dst.Ident{Name: Sel.Name, Path: fullImportPath}. This is the canonical dst
// form (what decorator produces when it has type information) and is what
// decorator.NewRestorerWithImports needs to auto-generate the correct import
// block. Setting Path on the X ident of a SelectorExpr instead causes a
// format.Node internal error ("expected ';', found '.'").
func AnnotateFieldIdents(field *dst.Field, importMap map[string]string) {
	field.Type = RewriteExpr(field.Type, importMap)
}

// RewriteExpr recursively rewrites dst.Expr nodes, converting
// SelectorExpr{X: pkgIdent, Sel: name} into Ident{Name: name, Path: pkgPath}.
func RewriteExpr(expr dst.Expr, importMap map[string]string) dst.Expr {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *dst.SelectorExpr:
		if x, ok := e.X.(*dst.Ident); ok && x.Path == "" {
			if fullPath, found := importMap[x.Name]; found {
				return &dst.Ident{Name: e.Sel.Name, Path: fullPath}
			}
		}
		e.X = RewriteExpr(e.X, importMap)
		return e
	case *dst.StarExpr:
		e.X = RewriteExpr(e.X, importMap)
		return e
	case *dst.ArrayType:
		e.Elt = RewriteExpr(e.Elt, importMap)
		return e
	case *dst.MapType:
		e.Key = RewriteExpr(e.Key, importMap)
		e.Value = RewriteExpr(e.Value, importMap)
		return e
	case *dst.IndexExpr:
		e.X = RewriteExpr(e.X, importMap)
		e.Index = RewriteExpr(e.Index, importMap)
		return e
	case *dst.IndexListExpr:
		e.X = RewriteExpr(e.X, importMap)
		for i, idx := range e.Indices {
			e.Indices[i] = RewriteExpr(idx, importMap)
		}
		return e
	case *dst.ChanType:
		e.Value = RewriteExpr(e.Value, importMap)
		return e
	case *dst.Ellipsis:
		e.Elt = RewriteExpr(e.Elt, importMap)
		return e
	case *dst.FuncType:
		for _, f := range e.Params.List {
			f.Type = RewriteExpr(f.Type, importMap)
		}
		if e.Results != nil {
			for _, f := range e.Results.List {
				f.Type = RewriteExpr(f.Type, importMap)
			}
		}
		return e
	default:
		return e
	}
}
