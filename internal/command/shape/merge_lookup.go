// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements syntax-tree lookups used by inplace merge to detect
// pre-existing package-level var and const declarations, including unexported ones that
// the type-checker based package model intentionally does not record.

package shape

import (
	"go/token"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/query/model"
)

// valueSpecMatch describes a top-level var/const ValueSpec located in a package file.
type valueSpecMatch struct {
	// File is the file that declares the value.
	File *dst.File
	// Decl is the GenDecl containing Spec (may hold several specs for grouped blocks).
	Decl *dst.GenDecl
	// Spec is the ValueSpec declaring the requested name.
	Spec *dst.ValueSpec
}

// findTopLevelValueSpec scans every file of the package for a top-level GenDecl with the
// given token (token.VAR or token.CONST) declaring name. Both single (`var x = 1`) and
// grouped (`var ( x = 1 )`) forms are matched. The lookup works on the DST rather than on
// pkg.Vars / pkg.Consts because the inspect pipeline only records exported identifiers;
// scanning the syntax also sees files created earlier in the same run by
// findOrCreateOutputFile. Returns nil when no declaration is found.
func findTopLevelValueSpec(pkg *model.Package, tok token.Token, name string) *valueSpecMatch {
	if pkg == nil || pkg.Package == nil || name == "" || name == "_" {
		return nil
	}
	for _, file := range pkg.Package.Syntax {
		if file == nil {
			continue
		}
		for _, decl := range file.Decls {
			gd, ok := decl.(*dst.GenDecl)
			if !ok || gd.Tok != tok {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*dst.ValueSpec)
				if !ok {
					continue
				}
				for _, ident := range vs.Names {
					if ident != nil && ident.Name == name {
						return &valueSpecMatch{File: file, Decl: gd, Spec: vs}
					}
				}
			}
		}
	}
	return nil
}

// valueSpecNames returns the non-blank identifiers declared by a ValueSpec.
func valueSpecNames(spec *dst.ValueSpec) []string {
	if spec == nil {
		return nil
	}
	names := make([]string, 0, len(spec.Names))
	for _, ident := range spec.Names {
		if ident == nil || ident.Name == "" || ident.Name == "_" {
			continue
		}
		names = append(names, ident.Name)
	}
	return names
}
