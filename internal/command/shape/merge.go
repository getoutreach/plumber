// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements inplace merging of generated struct fields into existing Go source files using the DST AST.

package shape

import (
	"fmt"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// Merge takes a model.Package and a dst.File representing the generated code, and merges the
// generated struct fields into the existing source file, returning the modified dst.File.
func Merge(pkg *model.Package, file *dst.File) (*dst.File, error) {
	importMap := astx.BuildImportMap(file)
	var currentFile *dst.File
	for _, decl := range file.Decls {
		if d, ok := decl.(*dst.GenDecl); ok {
			for _, spec := range d.Specs {
				if s, ok := spec.(*dst.TypeSpec); ok {
					switch t := s.Type.(type) {
					case *dst.StructType:
						currentType, found := lo.Find(pkg.Types, func(t *model.Type) bool {
							return t.Name == s.Name.Name
						})
						if !found {
							return nil, fmt.Errorf("type %q not found in package %q", s.Name.Name, pkg.Path)
						}
						f, err := mergeStruct(currentType, t, importMap)
						if err != nil {
							return nil, fmt.Errorf("failed to merge struct %q: %w", s.Name.Name, err)
						}
						currentFile = f
					default:
						return nil, fmt.Errorf("unsupported type declaration for %q in file %q: %T", s.Name.Name, file.Name, s.Type)
					}
				}
			}
		}
	}
	return currentFile, nil
}

// mergeStruct takes the current model.Type and the generated dst.StructType, and merges the fields from the generated struct
// into the existing struct in the source file, ensuring that existing fields are not overwritten and that new fields are
// properly annotated with imports.
func mergeStruct(current *model.Type, generated *dst.StructType, importMap map[string]string) (*dst.File, error) {
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

	// Build a set of existing field names in the target struct.
	existingNames := make(map[string]bool)
	if structType.Fields != nil {
		for _, field := range structType.Fields.List {
			for _, name := range fieldNames(field) {
				existingNames[name] = true
			}
		}
	}

	// Merge fields from the generated struct; skip if any name already exists.
	if generated.Fields != nil {
		for _, field := range generated.Fields.List {
			alreadyExists := lo.SomeBy(fieldNames(field), func(name string) bool {
				return existingNames[name]
			})
			if alreadyExists {
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
		if ident, ok := t.X.(*dst.Ident); ok {
			return []string{ident.Name}
		}
		if sel, ok := t.X.(*dst.SelectorExpr); ok {
			return []string{sel.Sel.Name}
		}
	case *dst.SelectorExpr:
		return []string{t.Sel.Name}
	}
	return nil
}
