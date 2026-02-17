// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: AST augmentation service for adding missing fields to container structs
// Managed: true

package discovery

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"os"
	"strings"
)

// AugmentResult contains information about the augmentation operation
type AugmentResult struct {
	Added   []string // Names of fields that were added
	Skipped []string // Names of fields that were skipped (already exist)
}

// Augmenter handles AST modifications to add missing fields to container structs
type Augmenter struct{}

// NewAugmenter creates a new augmenter instance
func NewAugmenter() *Augmenter {
	return &Augmenter{}
}

// AugmentContainerStruct adds missing fields to a container struct based on discovered source structs
// It uses the already-parsed AST from the parser
func (a *Augmenter) AugmentContainerStruct(
	containerPath string,
	containerName string,
	containerStruct *StructInfo,
	sourceResult *DiscoveryResult,
	file *ast.File,
	fset *token.FileSet,
) (*AugmentResult, error) {
	// Identify missing structs
	missingStructs := a.findMissingStructs(containerStruct, sourceResult)
	if len(missingStructs) == 0 {
		return &AugmentResult{}, nil
	}

	// Build import alias map from the file
	importAliases := a.buildImportAliasMap(file)

	// Find the container struct in the AST
	structDecl := a.findStructDecl(file, containerName)
	if structDecl == nil {
		return nil, fmt.Errorf("struct %q not found in file", containerName)
	}

	// Add missing fields to the struct
	result := a.addFieldsToStruct(structDecl, missingStructs, sourceResult, importAliases)

	// Write the modified AST back to the file
	if err := a.writeFile(containerPath, file, fset); err != nil {
		return nil, fmt.Errorf("failed to write file: %w", err)
	}

	return result, nil
}

// findMissingStructs identifies structs from source that are not in the container
func (a *Augmenter) findMissingStructs(container *StructInfo, sourceResult *DiscoveryResult) []*StructInfo {
	// Build a set of existing field names
	existingFields := make(map[string]bool)
	for _, field := range container.Fields {
		existingFields[field.Name] = true
	}

	// Find structs that don't have corresponding fields
	var missing []*StructInfo
	for _, sourceStruct := range sourceResult.Structs {
		if !existingFields[sourceStruct.Name] {
			missing = append(missing, sourceStruct)
		}
	}

	return missing
}

// buildImportAliasMap creates a map from package path to import alias
func (a *Augmenter) buildImportAliasMap(file *ast.File) map[string]string {
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

// findStructDecl finds a struct declaration by name in the AST
func (a *Augmenter) findStructDecl(file *ast.File, structName string) *ast.StructType {
	var structDecl *ast.StructType

	ast.Inspect(file, func(n ast.Node) bool {
		if genDecl, ok := n.(*ast.GenDecl); ok && genDecl.Tok == token.TYPE {
			for _, spec := range genDecl.Specs {
				if typeSpec, ok := spec.(*ast.TypeSpec); ok {
					if typeSpec.Name.Name == structName {
						if structType, ok := typeSpec.Type.(*ast.StructType); ok {
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

// addFieldsToStruct adds missing fields to a struct declaration
func (a *Augmenter) addFieldsToStruct(structDecl *ast.StructType, missingStructs []*StructInfo, sourceResult *DiscoveryResult, importAliases map[string]string) *AugmentResult {
	result := &AugmentResult{
		Added:   []string{},
		Skipped: []string{},
	}

	// Build constructor map for determining field types
	constructors := make(map[string]*ConstructorInfo)
	for _, ctor := range sourceResult.Constructors {
		// Extract struct name from return type (e.g., "*Publisher" -> "Publisher")
		structName := strings.TrimPrefix(ctor.ReturnType, "*")
		constructors[structName] = ctor
	}

	for _, sourceStruct := range missingStructs {
		// Determine field type based on constructor
		fieldType := a.determineFieldType(sourceStruct, constructors, importAliases)

		// Create new field
		field := &ast.Field{
			Names: []*ast.Ident{ast.NewIdent(sourceStruct.Name)},
			Type:  a.createTypeExpr(fieldType),
		}

		// Add field to struct - insert before the closing brace position
		structDecl.Fields.List = append(structDecl.Fields.List, field)
		result.Added = append(result.Added, sourceStruct.Name)
	}

	// Update the struct's closing position to ensure proper formatting
	if structDecl.Fields != nil && len(structDecl.Fields.List) > 0 {
		// Set the closing position after the last field
		structDecl.Fields.Closing = token.NoPos
	}

	return result
}

// determineFieldType determines the appropriate plumber wrapper type for a field
func (a *Augmenter) determineFieldType(sourceStruct *StructInfo, constructors map[string]*ConstructorInfo, importAliases map[string]string) string {
	// Find the import alias for the package
	pkgAlias := ""
	if sourceStruct.Package != "" {
		if alias, found := importAliases[sourceStruct.Package]; found {
			pkgAlias = alias + "."
		}
	}

	// Check if there's a constructor
	if ctor, hasConstructor := constructors[sourceStruct.Name]; hasConstructor {
		// Use plumber.R for structs with constructors (runnable)
		// Prepend the package alias to the return type
		returnType := ctor.ReturnType
		if !strings.Contains(returnType, ".") && pkgAlias != "" {
			// Add package alias to pointer types or regular types
			if strings.HasPrefix(returnType, "*") {
				returnType = "*" + pkgAlias + strings.TrimPrefix(returnType, "*")
			} else {
				returnType = pkgAlias + returnType
			}
		}
		return fmt.Sprintf("plumber.R[%s]", returnType)
	}

	// Use plumber.D for structs without constructors (dependency)
	return fmt.Sprintf("plumber.D[%s%s]", pkgAlias, sourceStruct.Name)
}

// createTypeExpr creates an AST type expression from a type string
// Manually builds the AST for types like plumber.R[*async.Publisher]
func (a *Augmenter) createTypeExpr(typeStr string) ast.Expr {
	// Parse pattern: pkg.Type[innerType]
	// Example: plumber.R[*async.Publisher]
	
	if !strings.Contains(typeStr, "[") {
		// Simple type without generics
		return a.parseSimpleType(typeStr)
	}
	
	// Find the generic wrapper and inner type
	bracketStart := strings.Index(typeStr, "[")
	bracketEnd := strings.LastIndex(typeStr, "]")
	
	if bracketStart == -1 || bracketEnd == -1 {
		// Fallback
		return ast.NewIdent(typeStr)
	}
	
	wrapperType := typeStr[:bracketStart]
	innerType := typeStr[bracketStart+1 : bracketEnd]
	
	// Build the indexed expression: pkg.Type[innerType]
	return &ast.IndexExpr{
		X:     a.parseSimpleType(wrapperType),
		Index: a.parseSimpleType(innerType),
	}
}

// parseSimpleType parses a simple type expression (possibly with package prefix or pointer)
func (a *Augmenter) parseSimpleType(typeStr string) ast.Expr {
	// Handle pointer
	if strings.HasPrefix(typeStr, "*") {
		return &ast.StarExpr{
			X: a.parseSimpleType(strings.TrimPrefix(typeStr, "*")),
		}
	}
	
	// Handle package-qualified type
	if strings.Contains(typeStr, ".") {
		parts := strings.SplitN(typeStr, ".", 2)
		return &ast.SelectorExpr{
			X:   ast.NewIdent(parts[0]),
			Sel: ast.NewIdent(parts[1]),
		}
	}
	
	// Simple identifier
	return ast.NewIdent(typeStr)
}

// writeFile writes the modified AST back to a file
func (a *Augmenter) writeFile(filepath string, file *ast.File, fset *token.FileSet) error {
	// Clear all position information to avoid conflicts
	ast.Inspect(file, func(n ast.Node) bool {
		if n != nil {
			// Reset positions for all nodes
			switch node := n.(type) {
			case *ast.Field:
				for _, name := range node.Names {
					name.NamePos = token.NoPos
				}
			case *ast.Ident:
				node.NamePos = token.NoPos
			}
		}
		return true
	})

	var buf bytes.Buffer
	
	// Create a new fileset for formatting
	newFset := token.NewFileSet()
	if err := format.Node(&buf, newFset, file); err != nil {
		return fmt.Errorf("failed to format AST: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filepath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	// Run format.Source for final cleanup
	content, err := os.ReadFile(filepath)
	if err != nil {
		return fmt.Errorf("failed to read file for formatting: %w", err)
	}

	formatted, err := format.Source(content)
	if err != nil {
		// If formatting fails, keep the unformatted version
		return nil
	}

	if err := os.WriteFile(filepath, formatted, 0644); err != nil {
		return fmt.Errorf("failed to write formatted file: %w", err)
	}

	return nil
}
