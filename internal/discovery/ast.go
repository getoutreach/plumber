// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: AST parser for Go source code analysis
// Managed: true

package discovery

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/tools/go/packages"
)

// DiscoveryResult contains the discovered types and constructors
type DiscoveryResult struct {
	Structs      []*StructInfo
	Constructors []*ConstructorInfo
}

// StructInfo contains information about a discovered struct
type StructInfo struct {
	Name       string
	TypeName   string // Fully qualified type name
	Fields     []*FieldInfo
	Comment    string
	File       string
	Package    string
}

// FieldInfo contains information about a struct field
type FieldInfo struct {
	Name     string
	TypeName string
	IsPublic bool
}

// ConstructorInfo contains information about a constructor function
type ConstructorInfo struct {
	Name       string
	ReturnType string
	Parameters []*ParameterInfo
	Comment    string
	File       string
	Package    string
}

// ParameterInfo contains information about a function parameter
type ParameterInfo struct {
	Name     string
	TypeName string
}

// ASTParser parses Go source files and extracts type information
type ASTParser struct {
	fset *token.FileSet
	pkgs []*packages.Package
}

// NewASTParser creates a new AST parser for the given paths
func NewASTParser(paths ...string) (*ASTParser, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("no paths provided")
	}

	// Get package directories from file paths
	pkgDirs := make(map[string]bool)
	for _, path := range paths {
		dir := filepath.Dir(path)
		pkgDirs[dir] = true
	}

	// Convert to slice
	dirs := make([]string, 0, len(pkgDirs))
	for dir := range pkgDirs {
		dirs = append(dirs, dir)
	}

	// Use the first directory as the working directory
	workDir := filepath.Dir(paths[0])

	cfg := &packages.Config{
		Mode: packages.NeedName |
			packages.NeedFiles |
			packages.NeedCompiledGoFiles |
			packages.NeedImports |
			packages.NeedTypes |
			packages.NeedSyntax |
			packages.NeedTypesInfo,
		Dir: workDir,
	}

	pkgs, err := packages.Load(cfg, dirs...)
	if err != nil {
		return nil, fmt.Errorf("failed to load packages: %w", err)
	}

	if len(pkgs) == 0 {
		return nil, fmt.Errorf("no packages found for paths: %v", paths)
	}

	// Check for errors in packages
	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			return nil, fmt.Errorf("package %q has errors: %v", pkg.PkgPath, pkg.Errors)
		}
	}

	return &ASTParser{
		fset: token.NewFileSet(),
		pkgs: pkgs,
	}, nil
}

// GetParsedFile returns the already-parsed AST file for a given path
func (p *ASTParser) GetParsedFile(filepath string) (*ast.File, error) {
	// Find the package that contains this file
	for _, pkg := range p.pkgs {
		for _, file := range pkg.Syntax {
			pos := pkg.Fset.Position(file.Pos())
			if pos.Filename == filepath {
				return file, nil
			}
		}
	}
	return nil, fmt.Errorf("file %q not found in parsed packages", filepath)
}

// GetFileSet returns the token file set used by the parser
func (p *ASTParser) GetFileSet() *token.FileSet {
	if len(p.pkgs) > 0 {
		return p.pkgs[0].Fset
	}
	return token.NewFileSet()
}

// Discover finds all structs and their constructors based on matchers
func (p *ASTParser) Discover(matchers []Matcher) (*DiscoveryResult, error) {
	return p.DiscoverInFiles(matchers, nil)
}

// DiscoverInFiles finds structs and constructors in specific files, or all files if fileFilter is nil
func (p *ASTParser) DiscoverInFiles(matchers []Matcher, fileFilter map[string]bool) (*DiscoveryResult, error) {
	result := &DiscoveryResult{
		Structs:      []*StructInfo{},
		Constructors: []*ConstructorInfo{},
	}

	for _, pkg := range p.pkgs {
		for _, file := range pkg.Syntax {
			// If file filter is provided, check if this file should be processed
			if fileFilter != nil {
				filePath := pkg.Fset.Position(file.Pos()).Filename
				if !fileFilter[filePath] {
					continue
				}
			}

			// Inspect the AST
			ast.Inspect(file, func(n ast.Node) bool {
				switch decl := n.(type) {
				case *ast.GenDecl:
					if decl.Tok == token.TYPE {
						p.processTypeDecl(pkg, decl, result)
					}
				case *ast.FuncDecl:
					p.processFuncDecl(pkg, file, decl, matchers, result)
				}
				return true
			})
		}
	}

	return result, nil
}

func (p *ASTParser) processTypeDecl(pkg *packages.Package, decl *ast.GenDecl, result *DiscoveryResult) {
	for _, spec := range decl.Specs {
		typeSpec, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}

		structType, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			continue
		}

		// Extract struct information
		structInfo := &StructInfo{
			Name:     typeSpec.Name.Name,
			TypeName: fmt.Sprintf("%s.%s", pkg.PkgPath, typeSpec.Name.Name),
			Fields:   []*FieldInfo{},
			Comment:  extractComment(decl.Doc),
			Package:  pkg.PkgPath,
		}

		// Extract fields
		for _, field := range structType.Fields.List {
			typeName := types.ExprString(field.Type)
			for _, name := range field.Names {
				structInfo.Fields = append(structInfo.Fields, &FieldInfo{
					Name:     name.Name,
					TypeName: typeName,
					IsPublic: ast.IsExported(name.Name),
				})
			}
		}

		result.Structs = append(result.Structs, structInfo)
	}
}

func (p *ASTParser) processFuncDecl(
	pkg *packages.Package,
	file *ast.File,
	decl *ast.FuncDecl,
	matchers []Matcher,
	result *DiscoveryResult,
) {
	// Check if this function matches any constructor pattern
	if !p.matchesConstructorPattern(decl.Name.Name, matchers) {
		return
	}

	// Extract return type
	if decl.Type.Results == nil || len(decl.Type.Results.List) == 0 {
		return
	}

	returnType := types.ExprString(decl.Type.Results.List[0].Type)

	// Extract parameters
	params := []*ParameterInfo{}
	if decl.Type.Params != nil {
		for _, param := range decl.Type.Params.List {
			typeName := types.ExprString(param.Type)
			for _, name := range param.Names {
				params = append(params, &ParameterInfo{
					Name:     name.Name,
					TypeName: typeName,
				})
			}
		}
	}

	constructorInfo := &ConstructorInfo{
		Name:       decl.Name.Name,
		ReturnType: returnType,
		Parameters: params,
		Comment:    extractComment(decl.Doc),
		Package:    pkg.PkgPath,
	}

	result.Constructors = append(result.Constructors, constructorInfo)
}

func (p *ASTParser) matchesConstructorPattern(funcName string, matchers []Matcher) bool {
	if len(matchers) == 0 {
		return true
	}

	for _, matcher := range matchers {
		if matcher.PlumberMatcherStruct != nil {
			for _, pattern := range matcher.PlumberMatcherStruct.Constructors {
				// Handle template patterns by converting to regex
				// Replace template variables with regex patterns
				regexPattern := pattern

				// Convert Go template syntax to regex patterns
				// {{ .name }} or {{ .name | capitalize }} -> .*
				// This matches any template variable regardless of filters
				regexPattern = regexp.MustCompile(`\{\{\s*\.[\w]+(\s*\|[\w\s]+)?\s*\}\}`).ReplaceAllString(regexPattern, "*")

				// Use filepath.Match for glob-style matching
				if matched, _ := filepath.Match(regexPattern, funcName); matched {
					return true
				}
			}
		}
	}

	return false
}

func extractComment(commentGroup *ast.CommentGroup) string {
	if commentGroup == nil {
		return ""
	}
	var lines []string
	for _, comment := range commentGroup.List {
		text := strings.TrimPrefix(comment.Text, "//")
		text = strings.TrimPrefix(text, "/*")
		text = strings.TrimSuffix(text, "*/")
		text = strings.TrimSpace(text)
		if text != "" {
			lines = append(lines, text)
		}
	}
	return strings.Join(lines, " ")
}

// ParseFile parses a single Go file and returns its AST
func ParseFile(path string) (*ast.File, *token.FileSet, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse file %q: %w", path, err)
	}
	return file, fset, nil
}
