// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements inplace merging of generated function and method declarations
// into existing Go source files, ensuring parameters and body statements are preserved.

package shape

import (
	"fmt"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

// mergeFunc merges a generated function declaration into the existing package.
// If the function does not exist, it is added entirely. If it exists, its parameters
// are augmented (template params must be present) and its body is merged statement-by-statement.
func mergeFunc(pkg *model.Package, generated *dst.FuncDecl, importMap map[string]string) (*dst.File, error) {
	// Find the existing function by name in the package.
	// For methods (functions with receivers), search the receiver type's methods.
	// For top-level functions, search pkg.Functions.
	existing := findExistingFunc(pkg, generated)

	if existing == nil {
		// Function not found — add it entirely to the first file in the package
		return addFunc(pkg, generated, importMap)
	}

	// Function exists — find it in the DST and merge
	file := pkg.File(existing.Position.Filename)
	if file == nil {
		return nil, fmt.Errorf(
			"file %q not found in package %q",
			existing.Position.Filename, pkg.Path,
		)
	}

	existingDecl := findFuncDecl(file, generated.Name.Name)
	if existingDecl == nil {
		return nil, fmt.Errorf(
			"function %q not found in DST of file %q",
			generated.Name.Name, existing.Position.Filename,
		)
	}

	// Merge parameters: ensure all template params are present
	if err := mergeParams(existingDecl, generated, importMap); err != nil {
		return nil, fmt.Errorf("failed to merge params for %q: %w", generated.Name.Name, err)
	}

	// Merge body: statement-by-statement subsequence merge
	if generated.Body != nil && len(generated.Body.List) > 0 {
		if err := mergeBody(existingDecl.Body, generated.Body, importMap); err != nil {
			return nil, fmt.Errorf("failed to merge body for %q: %w", generated.Name.Name, err)
		}
	}

	return file, nil
}

// addFunc appends a generated function declaration to the package. It picks the
// file where the receiver type is declared (for methods) or the first file.
func addFunc(pkg *model.Package, generated *dst.FuncDecl, importMap map[string]string) (*dst.File, error) {
	var targetFile *dst.File

	// For methods, try to find the file where the receiver type is declared
	if generated.Recv != nil && len(generated.Recv.List) > 0 {
		recvTypeName := receiverTypeName(generated.Recv.List[0])
		if recvTypeName != "" {
			recvType, found := lo.Find(pkg.Types, func(t *model.Type) bool {
				return t.Name == recvTypeName
			})
			if found {
				targetFile = pkg.File(recvType.Position.Filename)
			}
		}
	}

	// Fallback: use the first file in the package
	if targetFile == nil && len(pkg.Package.Syntax) > 0 {
		targetFile = pkg.Package.Syntax[0]
	}
	if targetFile == nil {
		return nil, fmt.Errorf("no file found in package %q to add function %q", pkg.Path, generated.Name.Name)
	}

	// Annotate all type expressions in the function for proper import resolution
	annotateFuncDecl(generated, importMap)

	// Append the function declaration
	targetFile.Decls = append(targetFile.Decls, generated)

	return targetFile, nil
}

// mergeParams ensures the existing function has at least the same parameters as the
// generated (template) function. Additional parameters in the existing function are fine.
func mergeParams(existing, generated *dst.FuncDecl, importMap map[string]string) error {
	if generated.Type.Params == nil || len(generated.Type.Params.List) == 0 {
		return nil
	}

	if existing.Type.Params == nil {
		existing.Type.Params = &dst.FieldList{}
	}

	// For each generated param, check if it's already present by matching type string.
	// We use positional matching: generated params should be a prefix of existing params.
	existingParams := existing.Type.Params.List
	generatedParams := generated.Type.Params.List

	for i, gp := range generatedParams {
		if i < len(existingParams) {
			// Param at this position exists — assume it matches (user may have renamed)
			continue
		}
		// Missing param — append it
		field := dst.Clone(gp).(*dst.Field)
		astx.AnnotateFieldIdents(field, importMap)
		existing.Type.Params.List = append(existing.Type.Params.List, field)
	}

	return nil
}

// findFuncDecl finds a function declaration by name in a dst.File.
func findFuncDecl(file *dst.File, name string) *dst.FuncDecl {
	for _, decl := range file.Decls {
		if fd, ok := decl.(*dst.FuncDecl); ok {
			if fd.Name.Name == name {
				return fd
			}
		}
	}
	return nil
}

// receiverTypeName extracts the type name from a method receiver field.
func receiverTypeName(recv *dst.Field) string {
	switch t := recv.Type.(type) {
	case *dst.Ident:
		return t.Name
	case *dst.StarExpr:
		if ident, ok := t.X.(*dst.Ident); ok {
			return ident.Name
		}
	}
	return ""
}

// findExistingFunc searches for an existing function/method in the package model.
// For methods (generated has a receiver), it searches the receiver type's method set.
// For top-level functions, it searches pkg.Functions.
func findExistingFunc(pkg *model.Package, generated *dst.FuncDecl) *model.Function {
	// For methods, search the receiver type's methods
	if generated.Recv != nil && len(generated.Recv.List) > 0 {
		recvTypeName := receiverTypeName(generated.Recv.List[0])
		if recvTypeName != "" {
			for _, t := range pkg.Types {
				if t.Name == recvTypeName && t.Struct != nil {
					for _, m := range t.Struct.Methods {
						if m.Name == generated.Name.Name {
							return m
						}
					}
				}
			}
		}
		return nil
	}

	// For top-level functions, search pkg.Functions
	fn, _ := lo.Find(pkg.Functions, func(f *model.Function) bool {
		return f.Name == generated.Name.Name
	})
	return fn
}

// annotateFuncDecl rewrites all type expressions in a function declaration
// so that package-qualified references use the canonical dst Ident.Path form.
func annotateFuncDecl(fd *dst.FuncDecl, importMap map[string]string) {
	if fd.Type.Params != nil {
		for _, f := range fd.Type.Params.List {
			astx.AnnotateFieldIdents(f, importMap)
		}
	}
	if fd.Type.Results != nil {
		for _, f := range fd.Type.Results.List {
			astx.AnnotateFieldIdents(f, importMap)
		}
	}
	if fd.Recv != nil {
		for _, f := range fd.Recv.List {
			astx.AnnotateFieldIdents(f, importMap)
		}
	}
	// Annotate body expressions
	if fd.Body != nil {
		annotateBlockStmt(fd.Body, importMap)
	}
}
