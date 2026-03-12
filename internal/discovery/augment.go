// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: AST augmentation service for adding missing fields to container structs
// Managed: true

package discovery

import (
	"bytes"
	"fmt"
	"go/format"
	"go/token"
	"go/types"
	"os"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/dave/dst/decorator/resolver/gopackages"
	"github.com/dave/dst/dstutil"
	"github.com/getoutreach/plumber/internal/discovery/contract"
	"github.com/getoutreach/plumber/internal/discovery/templates"
	"github.com/samber/lo"
)

// AugmentResult contains information about the augmentation operation
type AugmentResult struct {
	Added   []string // Names of fields that were added
	Skipped []string // Names of fields that were skipped (already exist)
}

// Augmenter handles AST modifications to add missing fields to container structs
type Augmenter struct{}

var instanceMethods = []string{"Instance", "InstanceError"}

// NewAugmenter creates a new augmenter instance
func NewAugmenter() *Augmenter {
	return &Augmenter{}
}

// AugmentContainerStruct adds missing fields to a container struct based on discovered providers
// It parses the container file, finds missing provider fields, and adds them
func (a *Augmenter) AugmentContainerStruct(
	containerPath string,
	containerName string,
	providers []*contract.Provider,
	file *dst.File,
	dec *decorator.Decorator,
	providerMap map[string]*contract.ProviderMapping,
) (*AugmentResult, error) {
	// Find the container struct in the AST
	structDecl := a.findStructDecl(file, containerName)
	if structDecl == nil {
		return nil, fmt.Errorf("struct %q not found in file", containerName)
	}

	var (
		result         = &AugmentResult{}
		neededPackages = make(map[string]bool)
	)

	// Identify missing providers by checking existing struct fields
	missingProviders := a.findMissingProvidersFromStruct(structDecl, providers)
	if len(missingProviders) > 0 {

		// Build import alias map from the file
		importAliases := a.buildImportAliasMap(file)

		// Add missing fields to the struct
		result, neededPackages = a.addFieldsToStruct(file, containerName, structDecl, missingProviders, importAliases, providerMap)

		// Add plumber import if not present
		a.ensureImport(file, "github.com/getoutreach/plumber")

		// Add context import for Define method
		a.ensureImport(file, "context")

		// Add imports for all packages used in the field types
		for pkgPath := range neededPackages {
			a.ensureImport(file, pkgPath)
		}
	}

	changed := a.ensureDependencyRequired(file, containerName)

	if changed || len(missingProviders) > 0 {
		// Write the modified AST back to the file
		if err := a.writeFile(containerPath, file, dec); err != nil {
			return nil, fmt.Errorf("failed to write file: %w", err)
		}
	}

	return result, nil
}

// findMissingProvidersFromStruct identifies providers that are not in the container struct
// by examining the struct's field list directly
func (a *Augmenter) findMissingProvidersFromStruct(structDecl *dst.StructType, providers []*contract.Provider) []*contract.Provider {
	// Build a set of existing field names from the struct
	existingFields := make(map[string]bool)
	if structDecl.Fields != nil {
		for _, field := range structDecl.Fields.List {
			for _, name := range field.Names {
				existingFields[name.Name] = true
			}
		}
	}

	// Find providers that don't have corresponding fields
	var missing []*contract.Provider
	for _, provider := range providers {
		if !existingFields[provider.Name] {
			missing = append(missing, provider)
		}
	}

	return missing
}

// buildImportAliasMap creates a map from package path to import alias
func (a *Augmenter) buildImportAliasMap(file *dst.File) map[string]string {
	aliases := make(map[string]string)

	for _, imp := range file.Imports {
		pkgPath := strings.Trim(imp.Path.Value, `"`)

		var alias string
		if imp.Name != nil {
			// Explicit alias
			alias = imp.Name.Name
		} else {
			// Default alias is the last part of the path
			parts := strings.Split(pkgPath, "/")
			alias = parts[len(parts)-1]
		}

		aliases[pkgPath] = alias
	}

	return aliases
}

// ensureImport ensures an import is present in the file
func (a *Augmenter) ensureImport(file *dst.File, importPath string) {
	// Check if import already exists
	for _, imp := range file.Imports {
		if strings.Trim(imp.Path.Value, `"`) == importPath {
			return
		}
	}

	// Add the import
	newImport := &dst.ImportSpec{
		Path: &dst.BasicLit{
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

		// Insert at the beginning of declarations (after package)
		file.Decls = append([]dst.Decl{newGenDecl}, file.Decls...)
	}
}

// findStructDecl finds a struct declaration by name in the AST
func (a *Augmenter) findStructDecl(file *dst.File, structName string) *dst.StructType {
	var structDecl *dst.StructType

	dst.Inspect(file, func(n dst.Node) bool {
		if genDecl, ok := n.(*dst.GenDecl); ok && genDecl.Tok.String() == "type" {
			for _, spec := range genDecl.Specs {
				if typeSpec, ok := spec.(*dst.TypeSpec); ok {
					if typeSpec.Name.Name == structName {
						if structType, ok := typeSpec.Type.(*dst.StructType); ok {
							structDecl = structType
							return false
						}
					}
				}
			}
		}
		return true
	})

	return structDecl
}

// addFieldsToStruct adds missing fields to a struct declaration based on discovered providers
// Returns the result and a set of package paths that need to be imported
func (a *Augmenter) addFieldsToStruct(
	file *dst.File,
	containerName string,
	structDecl *dst.StructType,
	missingProviders []*contract.Provider,
	importAliases map[string]string,
	providerMap map[string]*contract.ProviderMapping,
) (*AugmentResult, map[string]bool) {
	result := &AugmentResult{
		Added:   []string{},
		Skipped: []string{},
	}

	// Track packages that need to be imported
	neededPackages := make(map[string]bool)

	defineFuncDeclaration := templates.FuncDeclaration(file, "Define")

	for _, provider := range missingProviders {
		// Determine field type based on provider
		fieldTypeExpr := a.determineFieldTypeExpr(provider)

		// Collect packages that need to be imported from the provider's type
		if provider.Type != nil && provider.Type.TypeInfo != nil {
			a.collectPackagesFromType(provider.Type.TypeInfo.Type, neededPackages)
		}

		// Create new field
		field := &dst.Field{
			Names: []*dst.Ident{{Name: provider.Name}},
			Type:  fieldTypeExpr,
		}

		// Add decorations for better formatting
		field.Decs.Before = dst.NewLine

		// provider.Constructors

		resolve := "Resolve" // Default to Resolve
		if provider.Constructor != nil && provider.Constructor.ReturnsError() {
			resolve = "ResolveError"
		}

		// Add field to struct
		structDecl.Fields.List = append(structDecl.Fields.List, field)
		result.Added = append(result.Added, provider.Name)

		args := lo.Map(provider.Constructor.Parameters, func(param *contract.ParameterInfo, _ int) dst.Expr {
			if mapping, ok := providerMap[param.TypeInfo.Type.String()]; ok {
				mappedArgs := lo.Map(mapping.Providers, func(p *contract.ContainerProvider, _ int) dst.Expr {
					exp := providerPathExpr(p, containerName)
					return &dst.CallExpr{
						Fun: &dst.SelectorExpr{
							X: exp,
							Sel: &dst.Ident{
								Name: "Instance",
							},
						},
					}
				})
				if len(mappedArgs) == 1 {
					return mappedArgs[0]
				}
				c := &dst.CallExpr{
					Fun: &dst.Ident{
						Name: "OneOf",
						Path: "github.com/getoutreach/plumber/discovery",
					},
					Args: newLinedArguments(mappedArgs),
				}
				c.Decs.Before = dst.NewLine
				c.Decs.After = dst.NewLine
				return c
			} else {
				return &dst.CallExpr{
					Fun: &dst.IndexListExpr{
						Indices: []dst.Expr{templates.ToTypeDefinition(param.TypeInfo.Type)},
						X: &dst.Ident{
							Name: "Undefined",
							Path: "github.com/getoutreach/plumber/discovery",
						},
					},
				}
			}
		})

		resolver := templates.ContainerResolver(
			templates.SelectorExprNameReplace(map[string]string{
				"NAME":               provider.Name,
				"DEPENDANCY_PACKAGE": "async",
				"RESOLVE":            resolve,
			}),
			templates.IdentReplace(map[string]any{
				"DEPENDANCY_TYPE": templates.TypeDefinition(*provider.Type),
				"CONSTRUCTOR_FUNCTION": func(c *dstutil.Cursor) {
					c.Replace(&dst.CallExpr{
						Fun: &dst.Ident{
							Name: provider.Constructor.FunctionName,
							Path: provider.Constructor.Package},
						Args: args,
					})
				},
			}),
		)

		// Create a restorer with the import manager enabled, and print the result. As you can see, the
		// import block is automatically managed, and the Println ident is converted to a SelectorExpr:
		//r := decorator.NewRestorerWithImports("root", gopackages.New("."))
		//restoredFile, err := r.RestoreFile(file)

		f := templates.FuncDeclaration(resolver, "DependencyResolverResolve")

		defineFuncDeclaration.Body.List = append(defineFuncDeclaration.Body.List, f.Body.List...)
	}

	return result, neededPackages
}

// determineFieldTypeExpr determines the appropriate plumber wrapper type for a provider
// Returns a dst.Expr representing plumber.D[T] (or plumber.R[T] for runners)
func (a *Augmenter) determineFieldTypeExpr(provider *contract.Provider) dst.Expr {
	// Default to plumber.D wrapper
	wrapperSel := &dst.SelectorExpr{
		X:   &dst.Ident{Name: "plumber"},
		Sel: &dst.Ident{Name: "D"}, // Default to D (dependency)
	}

	// Determine the inner type from the provider's type
	var innerType dst.Expr
	if provider.Type != nil && provider.Type.TypeInfo != nil {
		// Create a qualifier that uses the package from the type info
		qualifier := func(pkg *types.Package) string {
			if pkg == nil {
				return ""
			}
			return pkg.Name()
		}

		innerType = TypeToExpr(provider.Type.TypeInfo.Type, qualifier)
	} else {
		// Fallback to using the provider name as the type
		innerType = &dst.Ident{Name: provider.Name}
	}

	// Create the generic type expression plumber.D[Type]
	return &dst.IndexExpr{
		X:     wrapperSel,
		Index: innerType,
	}
}

// collectPackagesFromType recursively collects all packages used in a type
func (a *Augmenter) collectPackagesFromType(typ types.Type, packages map[string]bool) {
	if typ == nil {
		return
	}

	switch t := typ.(type) {
	case *types.Named:
		if pkg := t.Obj().Pkg(); pkg != nil {
			packages[pkg.Path()] = true
		}
		// Check type arguments for generics
		if t.TypeArgs() != nil {
			for i := 0; i < t.TypeArgs().Len(); i++ {
				a.collectPackagesFromType(t.TypeArgs().At(i), packages)
			}
		}

	case *types.Pointer:
		a.collectPackagesFromType(t.Elem(), packages)

	case *types.Slice:
		a.collectPackagesFromType(t.Elem(), packages)

	case *types.Array:
		a.collectPackagesFromType(t.Elem(), packages)

	case *types.Map:
		a.collectPackagesFromType(t.Key(), packages)
		a.collectPackagesFromType(t.Elem(), packages)

	case *types.Chan:
		a.collectPackagesFromType(t.Elem(), packages)

	case *types.Struct:
		for i := 0; i < t.NumFields(); i++ {
			a.collectPackagesFromType(t.Field(i).Type(), packages)
		}

	case *types.Interface:
		for i := 0; i < t.NumMethods(); i++ {
			a.collectPackagesFromType(t.Method(i).Type(), packages)
		}
		for i := 0; i < t.NumEmbeddeds(); i++ {
			a.collectPackagesFromType(t.EmbeddedType(i), packages)
		}

	case *types.Signature:
		// Collect from parameters
		if params := t.Params(); params != nil {
			for i := 0; i < params.Len(); i++ {
				a.collectPackagesFromType(params.At(i).Type(), packages)
			}
		}
		// Collect from results
		if results := t.Results(); results != nil {
			for i := 0; i < results.Len(); i++ {
				a.collectPackagesFromType(results.At(i).Type(), packages)
			}
		}
	}
}

func (a *Augmenter) ensureDependencyRequired(file *dst.File, containerName string) bool {
	defineFuncDeclaration := templates.FuncDeclaration(file, "Define")

	resolvers := templates.FindNodes(defineFuncDeclaration, func(node dst.Node) (match, recurse bool) {
		return templates.FindOnly(templates.IsFuncCallTo(node, "Resolver"))
	})

	for _, resolver := range resolvers {
		requireFunc := templates.FindNode(resolver, func(node dst.Node) (match bool, recurse bool) {
			return templates.FindOnly(templates.IsFuncCallTo(node, "Require"))
		})
		thenFunc := templates.FindNode(resolver, func(node dst.Node) (match bool, recurse bool) {
			return templates.FindOnly(templates.IsFuncCallTo(node, "Then"))
		})

		thenCallback := templates.FindCallbackBody(thenFunc, 0)

		functionCalls := templates.FindNodes(thenCallback, func(node dst.Node) (match bool, recurse bool) {
			_, ok := node.(*dst.CallExpr)
			return ok, true
		})

		usedInstanceSelectorExpr := lo.Compact(
			lo.Map(functionCalls, func(thenCallNode dst.Node, _ int) dst.Node {
				thenCall := thenCallNode.(*dst.CallExpr)
				if sel, ok := thenCall.Fun.(*dst.SelectorExpr); ok {
					if lo.Contains(instanceMethods, sel.Sel.Name) {
						return sel
					}
					if sel.Sel.Name == "Instance" {
						fmt.Println("> ", sel.Sel.Name, sel.Sel.Path)
					} else {
						fmt.Println(sel.Sel.Name, sel.Sel.Path)
					}
				}
				return nil
			}))

		if rf, ok := requireFunc.(*dst.CallExpr); ok {
			if rf == nil {
				return false
			}
			if len(rf.Args) == 0 {
				fmt.Println("Adding args")
				rf.Args = lo.Map(usedInstanceSelectorExpr, func(sel dst.Node, _ int) dst.Expr {
					return &dst.UnaryExpr{
						Op: token.AND,
						X:  sel.(*dst.SelectorExpr),
					}
				})
			}
			return false
		}
	}

	return false

}

// writeFile writes the modified AST back to a file
func (a *Augmenter) writeFile(filepath string, file *dst.File, dec *decorator.Decorator) error {
	var buf bytes.Buffer

	// Use the working directory for the restorer
	workDir := "."
	if absPath, err := os.Getwd(); err == nil {
		workDir = absPath
	}

	r := decorator.NewRestorerWithImports("main", gopackages.New(workDir))
	restoredFile, err := r.RestoreFile(file)

	if err != nil {
		return fmt.Errorf("failed to restore file: %w", err)
	}

	if err := format.Node(&buf, r.Fset, restoredFile); err != nil {
		return fmt.Errorf("failed to format file: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filepath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}
