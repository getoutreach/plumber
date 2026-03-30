// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: AST parser for Go source code analysis
// Managed: true

package discovery

import (
	"fmt"
	"go/ast"
	"go/types"
	"regexp"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/discovery/contract"
)

// ASTParser parses Go source files and extracts type information
type ASTParser struct {
	*astx.Parser
}

// NewASTParser creates a new AST parser for the given paths
func NewASTParser(paths ...string) (*ASTParser, error) {
	parser, err := astx.NewParser(paths, astx.WithTypeInfo())
	if err != nil {
		return nil, fmt.Errorf("failed to create AST parser: %w", err)
	}
	return &ASTParser{
		Parser: parser,
	}, nil
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

	for _, pkg := range p.Packages() {
		for _, file := range pkg.Syntax {
			if fileFilter != nil {
				if !fileFilter[pkg.Decorator.Filenames[file]] {
					continue
				}
			}

			// Inspect the DST for function declarations
			dst.Inspect(file, func(n dst.Node) bool {
				if decl, ok := n.(*dst.FuncDecl); ok {
					ctor, providerName := p.processFuncDecl(pkg, file, decl, matchers)
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

func (p *ASTParser) parametersInfo(pkg *decorator.Package, params []*ast.Field) []*contract.ParameterInfo {
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
	pkg *decorator.Package,
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

	f := pkg.Decorator.Ast.Nodes[decl]

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
