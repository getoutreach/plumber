// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements inplace doc-merging for named Go types whose
// underlying form is neither a struct nor an interface. It covers basic-type
// definitions (e.g. `type MyString string`), composite types (slices, maps,
// channels, function types), generics, and type aliases. The existing source
// is the source of truth: the underlying type expression is never rewritten on
// collision; only the type-level doc comment is reconciled using the
// preserve-manual rule that struct and interface merges already use.

package shape

import (
	"fmt"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/query/model"
)

// mergeNamedType merges a generated TypeSpec onto an existing named type whose
// underlying form is neither *dst.StructType nor *dst.InterfaceType. The doc
// comment for `type Foo bar` lives on the containing GenDecl, while grouped
// `type ( ... )` declarations carry per-spec docs on the inner TypeSpec, so
// both sources/targets are considered. Mismatches between the existing and
// generated underlying expressions are intentionally tolerated silently — the
// existing definition is always preserved.
//
// The existing TypeSpec is located by a manual decl scan instead of via
// file.Scope.Lookup because freshly-created destination files (produced by
// findOrCreateOutputFile) have a nil Scope. The manual scan also keeps the
// merger independent of dst's optional scope machinery.
func mergeNamedType(
	current *model.Type, genDecl *dst.GenDecl, genSpec *dst.TypeSpec,
) (*dst.File, error) {
	pkg := current.GetPackage()

	file := pkg.File(current.Position.Filename)
	if file == nil {
		return nil, fmt.Errorf("file %q not found in package %q", current.Position.Filename, pkg.Path)
	}

	typeSpec, existingGenDecl := findTypeSpecInFile(file, current.Name)
	if typeSpec == nil {
		return nil, fmt.Errorf("AST node for type %q not found in file %q", current.Name, current.Position.Filename)
	}

	// Merge type-level doc comments. Same dual GenDecl/TypeSpec strategy as
	// for structs and interfaces so the merger works equally well for plain
	// `type Foo bar` declarations and grouped `type ( ... )` blocks.
	if genDecl != nil && existingGenDecl != nil {
		mergeDocComment(&existingGenDecl.Decs.Start, genDecl.Decs.Start)
	}
	if genSpec != nil {
		mergeDocComment(&typeSpec.Decs.Start, genSpec.Decs.Start)
	}

	return file, nil
}

// findTypeSpecInFile returns the first TypeSpec with the given name in file
// along with its containing GenDecl. Both values are nil when the type is not
// found. The function performs a single linear pass over file.Decls and is
// safe to call on dst.Files with a nil Scope.
func findTypeSpecInFile(file *dst.File, name string) (*dst.TypeSpec, *dst.GenDecl) {
	if file == nil || name == "" {
		return nil, nil
	}
	for _, decl := range file.Decls {
		gd, ok := decl.(*dst.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*dst.TypeSpec)
			if !ok {
				continue
			}
			if ts.Name != nil && ts.Name.Name == name {
				return ts, gd
			}
		}
	}
	return nil, nil
}
