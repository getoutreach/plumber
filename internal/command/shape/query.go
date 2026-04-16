// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the plumber:query annotation processor that searches for entities matching a regex pattern within a defined scope and populates annotated slice variables with compatible results via inplace DST manipulation.

package shape

import (
	"fmt"
	"go/token"
	"go/types"
	"regexp"
	"strings"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/query/model"
)

// QueryAnnotation holds parsed plumber:query annotation data.
type QueryAnnotation struct {
	// Pattern is the regex pattern for matching entity names (first positional arg).
	Pattern *regexp.Regexp
	// Scope is the package path or FQN of a type to search within.
	Scope string
	// Receiver is the variable name used to qualify field/method access in type-scoped queries.
	// Required when scope resolves to a named type (e.g., scope=".Registry" receiver="r").
	Receiver string
}

// QueryTarget represents a package-level variable annotated with plumber:query.
type QueryTarget struct {
	Var        *model.PackageVar
	Annotation QueryAnnotation
}

// QueryResult represents a matched entity name and its source package path.
type QueryResult struct {
	Name    string
	PkgPath string
}

// parseQueryAnnotation extracts the regex pattern and scope from a plumber:query annotation.
func parseQueryAnnotation(ann model.Annotation) (*QueryAnnotation, error) {
	if len(ann.Args) == 0 {
		return nil, fmt.Errorf("plumber:query requires a regex pattern as the first argument")
	}

	rawPattern := strings.Trim(ann.Args[0], `"`)
	pattern, err := regexp.Compile(rawPattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern %q: %w", rawPattern, err)
	}

	rawScope, ok := ann.NamedArgs["scope"]
	if !ok {
		return nil, fmt.Errorf("plumber:query requires a scope=<package_or_type_fqn> named argument")
	}
	scope := strings.Trim(rawScope, `"`)

	var receiver string
	if rawReceiver, ok := ann.NamedArgs["receiver"]; ok {
		receiver = strings.Trim(rawReceiver, `"`)
	}

	return &QueryAnnotation{
		Pattern:  pattern,
		Scope:    scope,
		Receiver: receiver,
	}, nil
}

// collectQueryTargets finds all package-level variables annotated with plumber:query
// and returns them as QueryTarget values.
func collectQueryTargets(pkgs model.Packages) ([]QueryTarget, error) {
	var targets []QueryTarget
	for _, pkg := range pkgs {
		for _, v := range pkg.Vars {
			ann := v.Annotations.Find(contract.OptionQuery)
			if ann == nil {
				continue
			}

			qa, err := parseQueryAnnotation(*ann)
			if err != nil {
				return nil, fmt.Errorf("variable %q at %s: %w", v.Name, v.Position.Filename, err)
			}

			targets = append(targets, QueryTarget{
				Var:        v,
				Annotation: *qa,
			})
		}
	}
	return targets, nil
}

// resolveScope loads the scope's types.Scope for searching.
// It supports five forms:
//   - Current package: "." — searches the package where the annotated variable is declared.
//   - Current package type: ".TypeName" — searches fields/methods of a type in the current package.
//   - Relative package: "./<relpath>" — resolves relative to the caller package path (e.g., "./sub" from "github.com/pkg" → "github.com/pkg/sub").
//   - Package scope: "github.com/pkg" — searches all exported entities in the package.
//   - Type scope: "github.com/pkg.TypeName" — searches fields/methods of a specific type.
func resolveScope(pkgs model.Packages, scope string, callerPkg *model.Package) (*types.Scope, *types.Named, error) {
	// "." means the package where the annotated variable lives.
	if scope == "." {
		if callerPkg == nil {
			return nil, nil, fmt.Errorf("scope \".\" used but caller package is not available")
		}
		return callerPkg.Package.Package.Types.Scope(), nil, nil
	}

	// ".TypeName" — type in the current package (starts with "." but not "./").
	if strings.HasPrefix(scope, ".") && !strings.HasPrefix(scope, "./") {
		if callerPkg == nil {
			return nil, nil, fmt.Errorf("scope %q used but caller package is not available", scope)
		}
		typeName := scope[1:]
		obj := callerPkg.Package.Package.Types.Scope().Lookup(typeName)
		if obj == nil {
			return nil, nil, fmt.Errorf("type %q not found in package %q", typeName, callerPkg.Path)
		}
		tn, ok := obj.(*types.TypeName)
		if !ok {
			return nil, nil, fmt.Errorf("%q in package %q is not a type", typeName, callerPkg.Path)
		}
		named, ok := tn.Type().(*types.Named)
		if !ok {
			return nil, nil, fmt.Errorf("type %q in package %q is not a named type", typeName, callerPkg.Path)
		}
		return nil, named, nil
	}

	// Relative path: "./<relpath>" resolves from the caller package path.
	if strings.HasPrefix(scope, "./") {
		if callerPkg == nil {
			return nil, nil, fmt.Errorf("relative scope %q used but caller package is not available", scope)
		}
		// Resolve relative to the caller's parent package.
		// "./providers" from "github.com/pkg/consumer" → "github.com/pkg/consumer/providers"
		// "../providers" from "github.com/pkg/consumer" → "github.com/pkg/providers"
		resolved := callerPkg.Path + "/" + strings.TrimPrefix(scope, "./")
		return resolveScope(pkgs, resolved, nil)
	}

	// Try to find as a package path first by checking all loaded packages.
	for _, pkg := range pkgs {
		if pkg.Path == scope {
			return pkg.Package.Package.Types.Scope(), nil, nil
		}
	}

	// Try to find as type FQN: "pkg/path.TypeName"
	// Find the last dot that separates package path from type name.
	lastDot := strings.LastIndex(scope, ".")
	if lastDot == -1 {
		return nil, nil, fmt.Errorf("scope %q not found: not a loaded package and not a type FQN", scope)
	}

	pkgPath := scope[:lastDot]
	typeName := scope[lastDot+1:]

	for _, pkg := range pkgs {
		if pkg.Path == pkgPath {
			obj := pkg.Package.Package.Types.Scope().Lookup(typeName)
			if obj == nil {
				return nil, nil, fmt.Errorf("type %q not found in package %q", typeName, pkgPath)
			}
			tn, ok := obj.(*types.TypeName)
			if !ok {
				return nil, nil, fmt.Errorf("%q in package %q is not a type", typeName, pkgPath)
			}
			named, ok := tn.Type().(*types.Named)
			if !ok {
				return nil, nil, fmt.Errorf("type %q in package %q is not a named type", typeName, pkgPath)
			}
			return nil, named, nil
		}
	}

	return nil, nil, fmt.Errorf("package %q (from scope %q) not found in loaded packages", pkgPath, scope)
}

// executeQuery finds all entities matching the query pattern within the resolved scope
// that are type-compatible with the variable's element type.
func executeQuery(pkgs model.Packages, target QueryTarget) ([]QueryResult, error) {
	scope, named, err := resolveScope(pkgs, target.Annotation.Scope, target.Var.GetPackage())
	if err != nil {
		return nil, err
	}

	// Type-scoped queries require a receiver to generate valid field/method access.
	if named != nil {
		if target.Annotation.Receiver == "" {
			return nil, fmt.Errorf("type-scoped query for variable %q requires a receiver=<varname> named argument", target.Var.Name)
		}
		if err := validateReceiver(target.Var.GetPackage(), target.Annotation.Receiver, named); err != nil {
			return nil, err
		}
	}

	varType := target.Var.VarType
	elemType := sliceElementType(varType)
	if elemType == nil {
		return nil, fmt.Errorf("variable %q is not a slice type", target.Var.Name)
	}

	var results []QueryResult

	if scope != nil {
		// Package scope: search all exported functions, types, and variables.
		for _, name := range scope.Names() {
			if !target.Annotation.Pattern.MatchString(name) {
				continue
			}
			obj := scope.Lookup(name)
			if !obj.Exported() {
				continue
			}
			if isAssignableTo(obj.Type(), elemType) {
				pkg := obj.Pkg()
				pkgPath := ""
				if pkg != nil {
					pkgPath = pkg.Path()
				}
				results = append(results, QueryResult{
					Name:    name,
					PkgPath: pkgPath,
				})
			}
		}
	} else if named != nil {
		// Type scope: search methods of the named type.
		for method := range named.Methods() {
			if !target.Annotation.Pattern.MatchString(method.Name()) {
				continue
			}
			if !method.Exported() {
				continue
			}
			if isAssignableTo(method.Type(), elemType) {
				pkg := method.Pkg()
				pkgPath := ""
				if pkg != nil {
					pkgPath = pkg.Path()
				}
				results = append(results, QueryResult{
					Name:    method.Name(),
					PkgPath: pkgPath,
				})
			}
		}

		// Also search fields if the underlying type is a struct.
		if st, ok := named.Underlying().(*types.Struct); ok {
			for i := 0; i < st.NumFields(); i++ {
				field := st.Field(i)
				if !target.Annotation.Pattern.MatchString(field.Name()) {
					continue
				}
				if !field.Exported() {
					continue
				}
				if isAssignableTo(field.Type(), elemType) {
					pkg := field.Pkg()
					pkgPath := ""
					if pkg != nil {
						pkgPath = pkg.Path()
					}
					results = append(results, QueryResult{
						Name:    field.Name(),
						PkgPath: pkgPath,
					})
				}
			}
		}
	}

	return results, nil
}

// sliceElementType returns the element type of a slice type, or nil if the type is not a slice.
func sliceElementType(t types.Type) types.Type {
	switch v := t.(type) {
	case *types.Slice:
		return v.Elem()
	case *types.Named:
		return sliceElementType(v.Underlying())
	default:
		return nil
	}
}

// isAssignableTo checks whether srcType is structurally assignable to the target element type.
// For function types, this means the signatures must be identical.
// For interface types, srcType must implement the interface.
// For other types, standard assignability rules apply.
func isAssignableTo(srcType, targetType types.Type) bool {
	// Direct assignability.
	if types.AssignableTo(srcType, targetType) {
		return true
	}

	// For function-typed variables, a *types.Func's type is its *types.Signature.
	// Check if the signature is identical to the target signature.
	if targetSig, ok := targetType.Underlying().(*types.Signature); ok {
		if srcSig, ok := srcType.(*types.Signature); ok {
			return types.IdenticalIgnoreTags(srcSig, targetSig)
		}
	}

	// For interface targets, check if srcType implements the interface.
	if targetIface, ok := targetType.Underlying().(*types.Interface); ok {
		return types.Implements(srcType, targetIface)
	}

	return false
}

// validateReceiver checks that the receiver variable exists in the caller package's scope
// and that its type (after pointer dereference) matches the scope's named type.
func validateReceiver(callerPkg *model.Package, receiverName string, named *types.Named) error {
	pkgScope := callerPkg.Package.Package.Types.Scope()
	obj := pkgScope.Lookup(receiverName)
	if obj == nil {
		return fmt.Errorf("receiver %q not found in package %q", receiverName, callerPkg.Path)
	}
	receiverType := obj.Type()
	// Dereference pointer — both var r Registry and var r *Registry are valid.
	if ptr, ok := receiverType.(*types.Pointer); ok {
		receiverType = ptr.Elem()
	}
	if !types.Identical(receiverType, named) {
		expectedFQN := astx.FQNFromGoType(named)
		actualFQN := astx.FQNFromGoType(obj.Type())
		return fmt.Errorf("receiver %q has type %s, expected %s", receiverName, actualFQN, expectedFQN)
	}
	return nil
}

// inflateVariable modifies the DST of the source file to populate the variable's
// composite literal with the query results.
func inflateVariable(pkg *model.Package, target QueryTarget, results []QueryResult) (*dst.File, error) {
	file := pkg.File(target.Var.Position.Filename)
	if file == nil {
		return nil, fmt.Errorf("file %q not found in package %q", target.Var.Position.Filename, pkg.Path)
	}

	varName := target.Var.Name

	// Find the variable declaration in the DST.
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*dst.GenDecl)
		if !ok || genDecl.Tok != token.VAR {
			continue
		}
		for _, spec := range genDecl.Specs {
			vs, ok := spec.(*dst.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range vs.Names {
				if name.Name != varName {
					continue
				}
				if i >= len(vs.Values) {
					continue
				}
				compLit, ok := vs.Values[i].(*dst.CompositeLit)
				if !ok {
					return nil, fmt.Errorf("variable %q value is not a composite literal", varName)
				}

				// Build the element expressions from query results.
				elts := make([]dst.Expr, 0, len(results))
				for _, r := range results {
					var expr dst.Expr
					if target.Annotation.Receiver != "" {
						// Type-scoped with receiver: use receiver.FieldOrMethodName
						expr = &dst.SelectorExpr{
							X:   &dst.Ident{Name: target.Annotation.Receiver},
							Sel: &dst.Ident{Name: r.Name},
						}
					} else if r.PkgPath != "" && r.PkgPath != pkg.Path {
						// External package: use qualified identifier.
						expr = &dst.Ident{
							Name: r.Name,
							Path: r.PkgPath,
						}
					} else {
						// Same package: use unqualified identifier.
						expr = &dst.Ident{
							Name: r.Name,
						}
					}
					elts = append(elts, expr)
				}

				compLit.Elts = elts
				return file, nil
			}
		}
	}

	return nil, fmt.Errorf("variable declaration %q not found in DST of file %q", varName, target.Var.Position.Filename)
}

// processQueries is the top-level function that processes all plumber:query annotations.
// It finds query-annotated variables, executes queries, and inflates variables in-place.
func processQueries(pkgs model.Packages) ([]*QueryOutput, error) {
	targets, err := collectQueryTargets(pkgs)
	if err != nil {
		return nil, fmt.Errorf("failed to collect query targets: %w", err)
	}

	if len(targets) == 0 {
		return nil, nil
	}

	var outputs []*QueryOutput

	for _, target := range targets {
		fmt.Printf("Processing query for variable %q (pattern=%s, scope=%s)\n",
			target.Var.Name, target.Annotation.Pattern, target.Annotation.Scope)

		results, err := executeQuery(pkgs, target)
		if err != nil {
			return nil, fmt.Errorf("query for variable %q failed: %w", target.Var.Name, err)
		}

		fmt.Printf("  Found %d matching entities\n", len(results))
		for _, r := range results {
			fmt.Printf("    - %s (pkg: %s)\n", r.Name, r.PkgPath)
		}

		pkg := target.Var.GetPackage()
		file, err := inflateVariable(pkg, target, results)
		if err != nil {
			return nil, fmt.Errorf("failed to inflate variable %q: %w", target.Var.Name, err)
		}

		outputs = append(outputs, &QueryOutput{
			Filename: target.Var.Position.Filename,
			File:     file,
			Package:  pkg,
		})
	}

	return outputs, nil
}

// QueryOutput represents the result of a query transformation, containing the modified DST file.
type QueryOutput struct {
	Filename string
	File     *dst.File
	Package  *model.Package
}
