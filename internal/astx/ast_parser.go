package astx

import (
	"fmt"
	"path/filepath"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"golang.org/x/tools/go/packages"
)

// Parser parses Go source files and extracts type information
type Parser struct {
	// dec  *decorator.Decorator
	pkgs []*decorator.Package
}

type ParserConfig struct {
	WorkingDir string
	Mode       packages.LoadMode
	Overlay    map[string][]byte
	BuildFlags []string
}

type ParserOption func(*ParserConfig)

func WithLoadMode(mode packages.LoadMode) ParserOption {
	return func(config *ParserConfig) {
		config.Mode = mode
	}
}

func WithTypeInfo() ParserOption {
	return WithLoadMode(packages.NeedName |
		packages.NeedFiles |
		packages.NeedCompiledGoFiles |
		packages.NeedImports |
		packages.NeedTypes |
		packages.NeedSyntax |
		packages.NeedTypesInfo)
}

func WithReplacement() ParserOption {
	return WithLoadMode(
		packages.NeedName |
			packages.NeedSyntax |
			packages.NeedTypesInfo |
			packages.NeedFiles)
}

func WithSyntax() ParserOption {
	return WithLoadMode(
		packages.NeedName |
			packages.NeedSyntax | packages.NeedFiles)
}

func WithOverlay(overlay map[string][]byte) ParserOption {
	return func(config *ParserConfig) {
		config.Overlay = overlay
	}
}

func WithBuildFlags(flags []string) ParserOption {
	return func(config *ParserConfig) {
		config.BuildFlags = flags
	}
}

func WithWorkingDir(dir string) ParserOption {
	return func(config *ParserConfig) {
		config.WorkingDir = dir
	}
}

// NewParser creates a new AST parser for the given paths
func NewParser(paths []string, options ...ParserOption) (*Parser, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("no paths provided")
	}

	opts := &ParserConfig{
		WorkingDir: "./",
	}
	for _, opt := range options {
		opt(opts)
	}

	// Get package directories from file paths
	pkgDirs := make(map[string]bool)
	for _, path := range paths {
		dir := filepath.Dir(path)
		pkgDirs[dir] = true
	}

	// Convert to slice
	dirs := make([]string, 0, len(pkgDirs)+1)
	for dir := range pkgDirs {
		dirs = append(dirs, dir)
	}

	// Use the first directory as the working directory
	workDir := filepath.Dir(opts.WorkingDir)

	cfg := &packages.Config{
		Mode:       opts.Mode,
		Dir:        workDir,
		Overlay:    opts.Overlay,
		BuildFlags: opts.BuildFlags,
	}

	pkgs, err := decorator.Load(cfg, dirs...)
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

	return &Parser{
		pkgs: pkgs,
	}, nil
}

func (p *Parser) Packages() []*decorator.Package {
	return p.pkgs
}

// GetParsedFile returns the already-parsed AST file for a given path, converted to dst
func (p *Parser) GetParsedFile(filepath string) (f *dst.File, pkg *decorator.Package, err error) {
	// Find the package that contains this file
	for _, pkg := range p.pkgs {
		for _, file := range pkg.Syntax {
			if pkg.Decorator.Filenames[file] == filepath {
				return file, pkg, nil
			}
		}
	}
	return nil, nil, fmt.Errorf("file %q not found in parsed packages", filepath)
}

// GetFileAndDecorator returns the dst.File and decorator for augmentation purposes
// astx.AddImport(pkg.Package, existingFile, imp.Name)
func (p *Parser) GetFileAndDecorator(filepath string) (*dst.File, *decorator.Package, *decorator.Decorator) {
	file, pkg, err := p.GetParsedFile(filepath)
	if err != nil {
		return nil, nil, nil
	}
	return file, pkg, decorator.NewDecoratorFromPackage(pkg.Package)
}
