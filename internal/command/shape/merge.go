// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements inplace merging of generated declarations into existing Go source files using the DST AST.

package shape

import (
	"fmt"
	"go/token"
	"path"
	"path/filepath"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/astx/inspect"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// annotationGenerateOnce is the annotation name that suppresses merging
// when the entity already exists in the target code.
const annotationGenerateOnce = "generate:once"

// Merge takes a model.Package and a dst.File representing the generated code, and merges the
// generated declarations into the existing source files, returning every dst.File that was
// modified. A single Merge call may touch multiple files because each entity (struct, func,
// var) can live in a different file within the package; new files may also be created on
// the fly when an entity does not yet exist anywhere in the package.
//
// Declarations annotated with generate:once are skipped if the entity already exists
// in the target package.
//
// The output parameter specifies the filename (relative to the package directory) to
// append new declarations to when the target entity is not found in the existing
// package. When empty, a default filename is derived from the package name. The same
// output filename is used for every newly-added declaration, so when several missing
// entities are added in one call they all land in the same created file.
// nolint: gocritic,gocyclo //Why: switch is not an option
func Merge(pkg *model.Package, file *dst.File, output string) ([]*dst.File, error) {
	importMap := astx.BuildImportMap(file)

	// fileSet records every file we touched while preserving the order in which they
	// were first encountered. Sub-mergers always return nil OR the destination file;
	// recordFile dedups subsequent visits and propagates the source imports exactly
	// once per destination file.
	var (
		ordered       []*dst.File
		seenFiles     = make(map[*dst.File]bool)
		mergedImports = make(map[*dst.File]bool)
	)
	recordFile := func(f *dst.File) {
		if f == nil {
			return
		}
		if !seenFiles[f] {
			seenFiles[f] = true
			ordered = append(ordered, f)
		}
		mergeImports(f, file, mergedImports)
	}

	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *dst.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *dst.TypeSpec:
					if t, ok := s.Type.(*dst.StructType); ok {
						// Check generate:once: if the struct already exists, skip merging.
						if hasGenerateOnce(d.Decs.Start) {
							_, found := lo.Find(pkg.Types, func(t *model.Type) bool {
								return t.Name == s.Name.Name
							})
							if found {
								continue
							}
						}

						currentType, found := lo.Find(pkg.Types, func(t *model.Type) bool {
							return t.Name == s.Name.Name
						})
						if !found {
							// Type not found — append the type declaration to the
							// file specified by output (creating it if needed).
							f, err := addTypeDecl(pkg, d, s, output, importMap)
							if err != nil {
								return nil, fmt.Errorf("failed to add type %q: %w", s.Name.Name, err)
							}
							recordFile(f)
							continue
						}
						f, err := mergeStruct(currentType, d, s, t, importMap)
						if err != nil {
							return nil, fmt.Errorf("failed to merge struct %q: %w", s.Name.Name, err)
						}
						recordFile(f)
					} else if t, ok := s.Type.(*dst.InterfaceType); ok {
						// Check generate:once: if the interface already exists, skip merging.
						if hasGenerateOnce(d.Decs.Start) {
							_, found := lo.Find(pkg.Types, func(t *model.Type) bool {
								return t.Name == s.Name.Name
							})
							if found {
								continue
							}
						}

						currentType, found := lo.Find(pkg.Types, func(t *model.Type) bool {
							return t.Name == s.Name.Name
						})
						if !found {
							// Interface not found — append to the output file.
							f, err := addTypeDecl(pkg, d, s, output, importMap)
							if err != nil {
								return nil, fmt.Errorf("failed to add type %q: %w", s.Name.Name, err)
							}
							recordFile(f)
							continue
						}
						f, err := mergeInterface(currentType, d, s, t, importMap)
						if err != nil {
							return nil, fmt.Errorf("failed to merge interface %q: %w", s.Name.Name, err)
						}
						recordFile(f)
					}
				case *dst.ValueSpec:
					if d.Tok == token.VAR {
						f, err := mergeVar(pkg, s, output, importMap)
						if err != nil {
							return nil, fmt.Errorf("failed to merge var: %w", err)
						}
						recordFile(f)
					}
				}
			}
		case *dst.FuncDecl:
			// Check generate:once: if the function already exists, skip merging.
			if hasGenerateOnce(d.Decs.Start) && findExistingFunc(pkg, d) != nil {
				continue
			}

			f, err := mergeFunc(pkg, d, output, importMap)
			if err != nil {
				return nil, fmt.Errorf("failed to merge func %q: %w", d.Name.Name, err)
			}
			recordFile(f)
		}
	}

	// Hydrate every touched file back into the package model so that any subsequent
	// transformer in the same run (or any caller invoking Merge again) sees the
	// freshly-added types/vars/functions and skips re-creating them. The hydration
	// is idempotent so it is safe to call on files that already contained the
	// declaration (existing entries are matched by name and skipped).
	for _, f := range ordered {
		inspect.HydrateFile(pkg, f)
	}
	return ordered, nil
}

// mergeImports copies every import declared in source into target, preserving aliases
// and de-duplicating by import path. The seen map records files that have already
// been processed so the work is performed at most once per (target, source) pair.
func mergeImports(target, source *dst.File, seen map[*dst.File]bool) {
	if target == nil || source == nil {
		return
	}
	if seen[target] {
		return
	}
	seen[target] = true
	for _, imp := range source.Imports {
		if imp == nil || imp.Path == nil {
			continue
		}
		importPath := imp.Path.Value
		// Strip surrounding quotes preserved from the source literal.
		if len(importPath) >= 2 && importPath[0] == '"' && importPath[len(importPath)-1] == '"' {
			importPath = importPath[1 : len(importPath)-1]
		}
		var alias string
		if imp.Name != nil && imp.Name.Name != "" && imp.Name.Name != "_" && imp.Name.Name != "." {
			alias = imp.Name.Name
		}
		astx.EnsureImportWithAlias(target, importPath, alias)
	}
}

// addTypeDecl appends a generated type declaration to the package. The target file is
// determined by the output filename (relative to the package directory). If a file with
// the matching basename already exists in the package, the declaration is appended to
// that file. Otherwise, a new dst.File is created, registered with the package, and the
// declaration is added to it.
func addTypeDecl(
	pkg *model.Package, gen *dst.GenDecl, spec *dst.TypeSpec, output string, importMap map[string]string,
) (*dst.File, error) {
	// Annotate the type expression for proper import resolution prior to insertion.
	if spec.Type != nil {
		spec.Type = astx.RewriteExpr(spec.Type, importMap)
	}

	// Build a single-spec GenDecl preserving the original token, decorations and doc.
	newDecl := &dst.GenDecl{
		Tok:   gen.Tok,
		Specs: []dst.Spec{spec},
	}
	newDecl.Decs.Start = append(newDecl.Decs.Start, gen.Decs.Start...)
	newDecl.Decs.Before = dst.EmptyLine
	newDecl.Decs.After = dst.EmptyLine

	targetFile := findOrCreateOutputFile(pkg, output)
	if targetFile == nil {
		return nil, fmt.Errorf("no target file available in package %q to add type %q", pkg.Path, spec.Name.Name)
	}

	targetFile.Decls = append(targetFile.Decls, newDecl)
	return targetFile, nil
}

// findOrCreateOutputFile returns a dst.File from the package whose registered filename
// matches the basename of output. When no such file exists, a new dst.File is created
// using the package's Name and registered in the decorator's filename map so subsequent
// lookups (and write-out) resolve to the requested output filename.
func findOrCreateOutputFile(pkg *model.Package, output string) *dst.File {
	if pkg == nil || pkg.Package == nil {
		return nil
	}

	wanted := path.Base(output)

	// Try to find an existing file in the package whose basename matches the requested output.
	if wanted != "" && wanted != "." && wanted != "/" {
		for _, f := range pkg.Package.Syntax {
			existing := pkg.Package.Decorator.Filenames[f]
			if existing == "" {
				continue
			}
			if path.Base(existing) == wanted {
				return f
			}
		}
	}

	// Determine the package name to use for the new file.
	pkgName := pkg.Name
	if pkgName == "" && len(pkg.Package.Syntax) > 0 {
		if firstName := pkg.Package.Syntax[0].Name; firstName != nil {
			pkgName = firstName.Name
		}
	}
	if pkgName == "" {
		pkgName = path.Base(pkg.Path)
	}

	newFile := &dst.File{
		Name: dst.NewIdent(pkgName),
	}

	// Compute the absolute filename for the new file. The model.Package.Dir field is
	// the authoritative source for the package's filesystem directory. EnsureDir()
	// resolves it on demand from the underlying decorator.Package when it has not been
	// pre-populated. As a last-resort defence we still fall back to deriving the
	// directory from a sibling file's registered filename.
	dir := pkg.EnsureDir()
	if dir == "" {
		for _, f := range pkg.Package.Syntax {
			if existing := pkg.Package.Decorator.Filenames[f]; existing != "" {
				dir = filepath.Dir(existing)
				break
			}
		}
	}
	filename := wanted
	if filename == "" {
		filename = "plumber_inplace.go"
	}
	absFilename := filepath.Join(dir, filename)

	// Register the new file with the package and decorator so it is treated as a real
	// member of the package by downstream restoration logic.
	pkg.Package.Syntax = append(pkg.Package.Syntax, newFile)
	if pkg.Package.Decorator != nil {
		if pkg.Package.Decorator.Filenames == nil {
			pkg.Package.Decorator.Filenames = make(map[*dst.File]string)
		}
		pkg.Package.Decorator.Filenames[newFile] = absFilename
	}

	return newFile
}

// hasGenerateOnce reports whether any of the given DST decorations contain a generate:once annotation.
func hasGenerateOnce(decs ...dst.Decorations) bool {
	annotations := inspect.AnnotationsFromDecs(decs...)
	for _, ann := range annotations {
		if ann.Name == annotationGenerateOnce {
			return true
		}
	}
	return false
}

// mergeStruct takes the current model.Type and the generated dst.StructType, and merges the fields from the generated struct
// into the existing struct in the source file, ensuring that existing fields are not overwritten and that new fields are
// properly annotated with imports. Doc comments on the type declaration and on individual fields are merged using the
// preserve-manual rule: generated docs are adopted only when the existing entity has no comment.
func mergeStruct(
	current *model.Type, genDecl *dst.GenDecl, genSpec *dst.TypeSpec,
	generated *dst.StructType, importMap map[string]string,
) (*dst.File, error) {
	pkg := current.GetPackage()

	file := pkg.File(current.Position.Filename)
	if file == nil {
		return nil, fmt.Errorf("file %q not found in package %q", current.Position.Filename, pkg.Path)
	}
	astObject := file.Scope.Lookup(current.Name)

	if astObject == nil {
		return nil, fmt.Errorf("AST node for type %q not found in file %q", current.Name, current.Position.Filename)
	}

	typeSpec, ok := astObject.Decl.(*dst.TypeSpec)
	if !ok {
		return nil, fmt.Errorf("AST node for type %q is not a type declaration in file %q", current.Name, current.Position.Filename)
	}

	if typeSpec.Type == nil {
		return nil, fmt.Errorf("type %q has no underlying type in file %q", current.Name, current.Position.Filename)
	}

	structType, ok := typeSpec.Type.(*dst.StructType)
	if !ok {
		return nil, fmt.Errorf("type %q is not a struct in file %q", current.Name, current.Position.Filename)
	}

	// Merge type-level doc comments. The doc for `type Foo struct {...}` lives on the
	// containing GenDecl. For grouped `type ( ... )` declarations the comment lives on
	// the inner TypeSpec instead, so both sources/targets are considered.
	if genDecl != nil {
		if existingGenDecl := findContainingGenDecl(file, typeSpec); existingGenDecl != nil {
			mergeDocComment(&existingGenDecl.Decs.Start, genDecl.Decs.Start)
		}
	}
	if genSpec != nil {
		mergeDocComment(&typeSpec.Decs.Start, genSpec.Decs.Start)
	}

	// Build a set of existing field names in the target struct.
	existingNames := make(map[string]bool)
	if structType.Fields != nil {
		for _, field := range structType.Fields.List {
			for _, name := range fieldNames(field) {
				existingNames[name] = true
			}
		}
	}

	// Merge fields from the generated struct; for fields whose name already exists,
	// merge the field-level doc comment using the preserve-manual rule. Inline
	// (Decs.End) comments on existing fields are intentionally left untouched.
	if generated.Fields != nil {
		for _, field := range generated.Fields.List {
			alreadyExists := lo.SomeBy(fieldNames(field), func(name string) bool {
				return existingNames[name]
			})
			if alreadyExists {
				for _, name := range fieldNames(field) {
					if existing := findFieldByName(structType.Fields, name); existing != nil {
						mergeDocComment(&existing.Decs.Start, field.Decs.Start)
						break
					}
				}
				continue
			}
			astx.AnnotateFieldIdents(field, importMap)
			structType.Fields.List = append(structType.Fields.List, field)
			for _, name := range fieldNames(field) {
				existingNames[name] = true
			}
		}
	}

	return file, nil
}

// mergeInterface takes the current model.Type (which has Interface set) and the generated
// dst.InterfaceType, and merges methods and embedded interfaces from the generated interface
// into the existing interface in the source file. Methods are deduplicated by name; embedded
// interfaces are deduplicated by their type expression key. Doc comments on the type
// declaration and on individual methods/embeds are merged using the preserve-manual rule:
// generated docs are adopted only when the existing entity has no comment.
func mergeInterface(
	current *model.Type, genDecl *dst.GenDecl, genSpec *dst.TypeSpec,
	generated *dst.InterfaceType, importMap map[string]string,
) (*dst.File, error) {
	pkg := current.GetPackage()

	file := pkg.File(current.Position.Filename)
	if file == nil {
		return nil, fmt.Errorf("file %q not found in package %q", current.Position.Filename, pkg.Path)
	}
	astObject := file.Scope.Lookup(current.Name)

	if astObject == nil {
		return nil, fmt.Errorf("AST node for type %q not found in file %q", current.Name, current.Position.Filename)
	}

	typeSpec, ok := astObject.Decl.(*dst.TypeSpec)
	if !ok {
		return nil, fmt.Errorf("AST node for type %q is not a type declaration in file %q", current.Name, current.Position.Filename)
	}

	if typeSpec.Type == nil {
		return nil, fmt.Errorf("type %q has no underlying type in file %q", current.Name, current.Position.Filename)
	}

	ifaceType, ok := typeSpec.Type.(*dst.InterfaceType)
	if !ok {
		return nil, fmt.Errorf("type %q is not an interface in file %q", current.Name, current.Position.Filename)
	}

	// Merge type-level doc comments. Same dual GenDecl/TypeSpec strategy as for structs.
	if genDecl != nil {
		if existingGenDecl := findContainingGenDecl(file, typeSpec); existingGenDecl != nil {
			mergeDocComment(&existingGenDecl.Decs.Start, genDecl.Decs.Start)
		}
	}
	if genSpec != nil {
		mergeDocComment(&typeSpec.Decs.Start, genSpec.Decs.Start)
	}

	// Separate existing entries into methods (named) and embeds (anonymous).
	existingMethodNames := make(map[string]bool)
	existingEmbeds := make(map[string]bool)
	if ifaceType.Methods != nil {
		for _, entry := range ifaceType.Methods.List {
			if len(entry.Names) > 0 {
				for _, n := range entry.Names {
					existingMethodNames[n.Name] = true
				}
			} else {
				existingEmbeds[embedKey(entry.Type)] = true
			}
		}
	}

	// Merge entries from generated interface; for duplicates, apply field-level doc merge.
	if generated.Methods != nil {
		for _, entry := range generated.Methods.List {
			if len(entry.Names) > 0 {
				// Named method — deduplicate by name.
				alreadyExists := lo.SomeBy(entry.Names, func(n *dst.Ident) bool {
					return existingMethodNames[n.Name]
				})
				if alreadyExists {
					for _, n := range entry.Names {
						if existing := findFieldByName(ifaceType.Methods, n.Name); existing != nil {
							mergeDocComment(&existing.Decs.Start, entry.Decs.Start)
							break
						}
					}
					continue
				}
			} else if existingEmbeds[embedKey(entry.Type)] {
				// Embedded interface — deduplicate by type expression.
				if existing := findFieldByEmbedKey(ifaceType.Methods, embedKey(entry.Type)); existing != nil {
					mergeDocComment(&existing.Decs.Start, entry.Decs.Start)
				}
				continue
			}
			astx.AnnotateFieldIdents(entry, importMap)
			if ifaceType.Methods == nil {
				ifaceType.Methods = &dst.FieldList{}
			}
			ifaceType.Methods.List = append(ifaceType.Methods.List, entry)
			// Update tracking sets.
			if len(entry.Names) > 0 {
				for _, n := range entry.Names {
					existingMethodNames[n.Name] = true
				}
			} else {
				existingEmbeds[embedKey(entry.Type)] = true
			}
		}
	}

	return file, nil
}

// embedKey returns a normalized string key for a type expression used as an embedded
// interface. It handles both the fully-decorated form (Ident with Path set, produced
// by go/packages decorator) and the plain parsed form (SelectorExpr, produced by
// decorator.Parse without type info).
func embedKey(expr dst.Expr) string {
	if expr == nil {
		return ""
	}
	switch t := expr.(type) {
	case *dst.Ident:
		if t.Path != "" {
			return t.Path + "." + t.Name
		}
		return t.Name
	case *dst.SelectorExpr:
		if x, ok := t.X.(*dst.Ident); ok {
			pkg := x.Path
			if pkg == "" {
				pkg = x.Name
			}
			return pkg + "." + t.Sel.Name
		}
		return exprKey(expr)
	case *dst.StarExpr:
		return "*" + embedKey(t.X)
	case *dst.IndexExpr:
		return embedKey(t.X) + "[" + embedKey(t.Index) + "]"
	default:
		return exprKey(expr)
	}
}

// fieldNames returns the effective names for a struct field.
// For named fields it returns the declared names; for embedded (anonymous)
// fields it derives the name from the type expression.
func fieldNames(field *dst.Field) []string {
	if len(field.Names) > 0 {
		return lo.Map(field.Names, func(ident *dst.Ident, _ int) string {
			return ident.Name
		})
	}
	// Embedded field – derive name from type.
	switch t := field.Type.(type) {
	case *dst.Ident:
		return []string{t.Name}
	case *dst.StarExpr:
		// Pointer to embedded type — recurse to handle all inner forms
		// (ident, selector, generic, etc.)
		return fieldNames(&dst.Field{Type: t.X})
	case *dst.SelectorExpr:
		return []string{t.Sel.Name}
	case *dst.IndexExpr:
		// Generic embedded field with single type param, e.g. GenericType[int]
		return fieldNames(&dst.Field{Type: t.X})
	case *dst.IndexListExpr:
		// Generic embedded field with multiple type params, e.g. GenericType[int, string]
		return fieldNames(&dst.Field{Type: t.X})
	}
	return nil
}
