// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: AST parser for Go source code analysis
// Managed: true

package discovery

import (
	"fmt"
	"go/ast"
	"go/types"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/discovery/contract"
	"golang.org/x/tools/go/packages"
)

// ASTParser parses Go source files and extracts type information
type ASTParser struct {
	dec  *decorator.Decorator
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

	// Create decorator for converting ast to dst
	dec := decorator.NewDecorator(pkgs[0].Fset)

	return &ASTParser{
		dec:  dec,
		pkgs: pkgs,
	}, nil
}

// GetParsedFile returns the already-parsed AST file for a given path, converted to dst
func (p *ASTParser) GetParsedFile(filepath string) (*dst.File, error) {
	// Find the package that contains this file
	for _, pkg := range p.pkgs {
		for _, file := range pkg.Syntax {
			pos := pkg.Fset.Position(file.Pos())
			if pos.Filename == filepath {
				// Convert ast.File to dst.File
				dstFile, err := p.dec.DecorateFile(file)
				if err != nil {
					return nil, fmt.Errorf("failed to convert to dst: %w", err)
				}
				return dstFile, nil
			}
		}
	}
	return nil, fmt.Errorf("file %q not found in parsed packages", filepath)
}

// GetFileAndDecorator returns the dst.File and decorator for augmentation purposes
func (p *ASTParser) GetFileAndDecorator(filepath string) (*dst.File, *decorator.Decorator) {
	file, err := p.GetParsedFile(filepath)
	if err != nil {
		return nil, nil
	}
	return file, p.dec
}

// GetDecorator returns the decorator used by the parser
func (p *ASTParser) GetDecorator() *decorator.Decorator {
	return p.dec
}

// Discover finds all structs and their constructors based on matchers
func (p *ASTParser) Discover(matchers []Matcher) (*contract.DiscoveryResult, error) {
	return p.DiscoverInFiles(matchers, nil)
}

// DiscoverInFiles finds constructors in specific files, or all files if fileFilter is nil
func (p *ASTParser) DiscoverInFiles(matchers []Matcher, fileFilter map[string]bool) (*contract.DiscoveryResult, error) {
	// First, collect all constructors
	constructors := []*contract.ConstructorInfo{}
	providerNames := make(map[string]string) // funcName -> providerName

	for _, pkg := range p.pkgs {
		for _, file := range pkg.Syntax {
			// If file filter is provided, check if this file should be processed
			if fileFilter != nil {
				filePath := pkg.Fset.Position(file.Pos()).Filename
				if !fileFilter[filePath] {
					continue
				}
			}

			// Convert ast.File to dst.File for inspection
			dstFile, err := p.dec.DecorateFile(file)
			if err != nil {
				// Skip files that can't be converted
				continue
			}

			// Inspect the DST for function declarations
			dst.Inspect(dstFile, func(n dst.Node) bool {
				if decl, ok := n.(*dst.FuncDecl); ok {
					ctor, providerName := p.processFuncDecl(pkg, dstFile, decl, matchers)
					if ctor != nil {
						constructors = append(constructors, ctor)
						if providerName != "" {
							providerNames[ctor.FunctionName] = providerName
						}
					}
				}
				return true
			})
		}
	}

	// Build providers from constructors
	result := &contract.DiscoveryResult{
		Providers: p.buildProviders(constructors, providerNames),
	}

	return result, nil
}

func (p *ASTParser) parametersInfo(pkg *packages.Package, params []*ast.Field) []*contract.ParameterInfo {
	results := []*contract.ParameterInfo{}
	for _, res := range params {
		tp := pkg.TypesInfo.TypeOf(res.Type)
		names := res.Names
		if len(names) == 0 {
			// If there are no names, we can create a synthetic one for the return value
			names = []*ast.Ident{{Name: ""}}
		}
		for _, name := range names {
			results = append(results, &contract.ParameterInfo{
				Name:     name.Name,
				TypeName: types.TypeString(tp, types.RelativeTo(pkg.Types)),
				TypeInfo: &contract.TypeInfo{
					Package: pkg,
					Type:    tp,
				},
			})
		}
	}
	return results
}

func (p *ASTParser) processFuncDecl(
	pkg *packages.Package,
	file *dst.File,
	decl *dst.FuncDecl,
	matchers []Matcher,
) (*contract.ConstructorInfo, string) {
	// Check if this function matches any constructor pattern
	providerName, matched := p.matchConstructorPattern(decl.Name.Name, matchers)
	if !matched {
		return nil, ""
	}

	// Extract return type only for functions has one or two return values (common for constructors)
	if decl.Type.Results == nil || len(decl.Type.Results.List) == 0 || len(decl.Type.Results.List) > 2 {
		return nil, ""
	}

	if decl.Recv != nil {
		// Skip methods
		return nil, ""
	}

	f := p.dec.Ast.Nodes[decl]

	var (
		results = []*contract.ParameterInfo{}
		params  = []*contract.ParameterInfo{}
	)

	if afd, ok := f.(*ast.FuncDecl); ok {
		results = p.parametersInfo(pkg, afd.Type.Results.List)
		params = p.parametersInfo(pkg, afd.Type.Params.List)
	}

	constructorInfo := &contract.ConstructorInfo{
		FunctionName:     decl.Name.Name,
		Parameters:       params,
		ReturnParameters: results,
		ReturnType:       results[0],
		Comment:          extractComment(decl.Decs.Start),
		Package:          pkg.PkgPath,
	}

	return constructorInfo, providerName
}

// buildProviders groups constructors by provider name and creates Provider entities
// Deduplicates providers with the same name and type
func (p *ASTParser) buildProviders(constructors []*contract.ConstructorInfo, providerNames map[string]string) []*contract.Provider {
	providerMap := make(map[string]*contract.Provider)

	for _, ctor := range constructors {
		providerName := providerNames[ctor.FunctionName]

		// If no provider name, skip (constructor without named group match)
		if providerName == "" {
			continue
		}

		// Check if provider already exists
		if _, exists := providerMap[providerName]; exists {
			continue
		} else {
			// Create new provider
			provider := &contract.Provider{
				Name:        providerName,
				Type:        ctor.ReturnType,
				Constructor: ctor,
			}
			providerMap[providerName] = provider
		}
	}

	// Convert map to slice
	providers := make([]*contract.Provider, 0, len(providerMap))
	for _, provider := range providerMap {
		providers = append(providers, provider)
	}

	return providers
}

// matchConstructorPattern checks if a function name matches any constructor pattern
// Returns (providerName, matched) where providerName is extracted from the "name" capture group
func (p *ASTParser) matchConstructorPattern(funcName string, matchers []Matcher) (string, bool) {
	if len(matchers) == 0 {
		return "", true
	}

	for _, matcher := range matchers {
		for _, pattern := range matcher.Constructors {
			// Pattern uses named capture groups: New(?P<name>.*) or Factory(?P<name>.*)
			re, err := regexp.Compile(pattern)
			if err != nil {
				continue
			}

			matches := re.FindStringSubmatch(funcName)
			if matches != nil {
				// Extract the "name" capture group if present
				for i, groupName := range re.SubexpNames() {
					if groupName == "name" && i < len(matches) {
						return matches[i], true
					}
				}
				// Match found but no named group
				return "", true
			}
		}
	}

	return "", false
}

func extractComment(decorations dst.Decorations) string {
	var lines []string
	for _, comment := range decorations.All() {
		text := strings.TrimPrefix(comment, "//")
		text = strings.TrimPrefix(text, "/*")
		text = strings.TrimSuffix(text, "*/")
		text = strings.TrimSpace(text)
		if text != "" {
			lines = append(lines, text)
		}
	}
	return strings.Join(lines, " ")
}

// ParseFile parses a single Go file and returns its DST
func ParseFile(path string) (*dst.File, *decorator.Decorator, error) {
	dec := decorator.NewDecoratorWithImports(nil, "", nil)
	file, err := dec.ParseFile(path, nil, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse file %q: %w", path, err)
	}
	return file, dec, nil
}

// renderType converts a dst.Expr to a string representation
func renderType(expr dst.Expr) string {
	if expr == nil {
		return ""
	}

	switch t := expr.(type) {
	case *dst.Ident:
		return t.Name
	case *dst.StarExpr:
		return "*" + renderType(t.X)
	case *dst.SelectorExpr:
		return renderType(t.X) + "." + t.Sel.Name
	case *dst.ArrayType:
		return "[]" + renderType(t.Elt)
	case *dst.MapType:
		return "map[" + renderType(t.Key) + "]" + renderType(t.Value)
	case *dst.InterfaceType:
		return "interface{}"
	case *dst.IndexExpr:
		// Generic type like Type[T]
		return renderType(t.X) + "[" + renderType(t.Index) + "]"
	case *dst.IndexListExpr:
		// Generic type with multiple parameters like Type[T, U]
		params := make([]string, len(t.Indices))
		for i, idx := range t.Indices {
			params[i] = renderType(idx)
		}
		return renderType(t.X) + "[" + strings.Join(params, ", ") + "]"
	default:
		return "unknown"
	}
}
