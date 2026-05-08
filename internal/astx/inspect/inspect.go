// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements Go source file scanning and AST inspection to extract types, functions,
// and annotations into the query model.

// Package inspect provides utilities for scanning Go source files and extracting type and annotation
// information into the plumber query model.
package inspect

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"io/fs"
	"iter"
	"path"
	"path/filepath"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

func ScanFiles(baseDir string, args []string) (filenames []string, err error) {
	baseDir, err = filepath.Abs("./")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve config path: %w", err)
	}

	for _, arg := range args {
		recursive := strings.HasSuffix(arg, "/...")
		if recursive {
			arg = strings.TrimSuffix(arg, "/...")
		}
		level := strings.HasSuffix(arg, "/")
		if level {
			arg = strings.TrimSuffix(arg, "/")
		}

		walk := func(s string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && !recursive {
				if level && s == "." {
					return nil
				}
				return fs.SkipDir
			}
			if !d.IsDir() && strings.HasSuffix(s, ".go") {
				filenames = append(filenames, path.Join(baseDir, s))
			}
			return nil
		}
		err := filepath.WalkDir(arg, walk)
		if err != nil {
			return nil, fmt.Errorf("failed to walk directory %q: %w", arg, err)
		}
	}

	return filenames, nil
}

func Inspect(filenames []string, workingDir string) (pkgs model.Packages, err error) {
	parser, err := astx.NewParser(filenames, astx.WithTypeInfo(), astx.WithWorkingDir(workingDir))
	if err != nil {
		return nil, fmt.Errorf("failed to create parser: %w", err)
	}
	for _, pkg := range parser.Packages() {
		pm := &model.Package{
			Package: pkg,
			Name:    pkg.Name,
			Path:    pkg.PkgPath,
		}
		// Resolve and cache the absolute filesystem directory now so downstream
		// consumers can rely on pm.Dir being populated.
		pm.EnsureDir()

		for _, file := range pkg.Package.Syntax {
			pm.Comments = append(pm.Comments, processComments(pm, file)...)
		}

		processScope(pkg.Package.Types.Scope(), pm)
		pkgs = append(pkgs, pm)
	}
	return pkgs, nil
}

// HydrateFile walks the supplied dst.File and appends minimal model entries (Type, PackageVar,
// Function/method) to the package model for every top-level declaration that isn't already
// present. It is meant to be called after Merge() injects new declarations into a file (or
// creates a new file via findOrCreateOutputFile) so that subsequent transformers in the same
// run see the newly-added entities and don't try to create them again.
//
// Hydration is intentionally lightweight: only the fields needed by the merge-side lookups
// (Name, Position.Filename, Struct/Interface presence, methods) are populated. Re-running
// the full type-checker would be required to populate go/types information; that is out of
// scope and not needed for the current call sites.
func HydrateFile(pkg *model.Package, file *dst.File) {
	if pkg == nil || file == nil {
		return
	}

	// Resolve the on-disk filename for this file (used to populate Position so the
	// receiver-type → file lookup in addFunc can find this file via pkg.File()).
	var filename string
	if pkg.Package != nil && pkg.Package.Decorator != nil {
		filename = pkg.Package.Decorator.Filenames[file]
	}

	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *dst.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *dst.TypeSpec:
					hydrateTypeSpec(pkg, s, filename)
				case *dst.ValueSpec:
					if d.Tok == token.VAR {
						hydrateValueSpec(pkg, s, filename)
					}
				}
			}
		case *dst.FuncDecl:
			hydrateFuncDecl(pkg, d, filename)
		}
	}
}

// hydrateTypeSpec appends a model.Type for a struct/interface declaration if it is not
// already present in pkg.Types.
func hydrateTypeSpec(pkg *model.Package, spec *dst.TypeSpec, filename string) {
	if spec == nil || spec.Name == nil {
		return
	}
	name := spec.Name.Name
	if _, found := lo.Find(pkg.Types, func(t *model.Type) bool {
		return t.Name == name
	}); found {
		return
	}

	tp := &model.Type{
		TypeNode: &model.TypeNode{
			Package:  pkg,
			Position: model.Position{Filename: filename},
		},
		Name: name,
	}
	switch spec.Type.(type) {
	case *dst.StructType:
		tp.Struct = &model.Struct{}
	case *dst.InterfaceType:
		tp.Interface = &model.Interface{}
	}
	pkg.Types = append(pkg.Types, tp)
}

// hydrateValueSpec appends a model.PackageVar for each name declared in the value spec
// if the variable is not already present in pkg.Vars.
func hydrateValueSpec(pkg *model.Package, spec *dst.ValueSpec, filename string) {
	for _, ident := range spec.Names {
		if ident == nil || ident.Name == "" {
			continue
		}
		name := ident.Name
		if _, found := lo.Find(pkg.Vars, func(v *model.PackageVar) bool {
			return v.Name == name
		}); found {
			continue
		}
		pkg.Vars = append(pkg.Vars, &model.PackageVar{
			TypeNode: model.TypeNode{
				Package:  pkg,
				Position: model.Position{Filename: filename},
			},
			Name: name,
		})
	}
}

// hydrateFuncDecl appends a model.Function for a top-level function or attaches a method
// to the receiver type's Struct.Methods. Existing entries (matched by name) are skipped.
func hydrateFuncDecl(pkg *model.Package, decl *dst.FuncDecl, filename string) {
	if decl == nil || decl.Name == nil {
		return
	}
	name := decl.Name.Name

	fn := &model.Function{
		TypeNode: model.TypeNode{
			Package:  pkg,
			Position: model.Position{Filename: filename},
		},
		Name: name,
	}

	// Method: attach to the receiver type's Struct.Methods.
	if decl.Recv != nil && len(decl.Recv.List) > 0 {
		recvName := dstReceiverTypeName(decl.Recv.List[0])
		if recvName == "" {
			return
		}
		recvType, found := lo.Find(pkg.Types, func(t *model.Type) bool {
			return t.Name == recvName
		})
		if !found {
			return
		}
		if recvType.Struct == nil {
			recvType.Struct = &model.Struct{}
		}
		if _, found := lo.Find(recvType.Struct.Methods, func(m *model.Function) bool {
			return m.Name == name
		}); found {
			return
		}
		recvType.Struct.Methods = append(recvType.Struct.Methods, fn)
		return
	}

	// Top-level function: skip duplicates, then append.
	if _, found := lo.Find(pkg.Functions, func(f *model.Function) bool {
		return f.Name == name
	}); found {
		return
	}
	pkg.Functions = append(pkg.Functions, fn)
}

// dstReceiverTypeName extracts the receiver type's bare name from a dst method receiver
// field. Mirrors merge_func.receiverTypeName (kept private to avoid a circular dep).
func dstReceiverTypeName(recv *dst.Field) string {
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

func processComments(pkg *model.Package, f *ast.File) []*model.CommentGroup {
	var comments []*model.CommentGroup

	for _, cg := range f.Comments {
		txt := strings.TrimSpace(cg.Text())
		if !strings.HasPrefix(txt, "@comment") {
			continue
		}
		txt = strings.TrimPrefix(txt, "@comment")
		txt = strings.TrimSpace(txt)

		cg.Pos()

		comments = append(comments, &model.CommentGroup{
			Doc:         txt,
			Annotations: ParseAnnotations(txt),
			Position: model.Position{
				Filename: pkg.Package.Fset.Position(cg.Pos()).Filename,
				Line:     pkg.Package.Fset.Position(cg.Pos()).Line,
				Column:   pkg.Package.Fset.Position(cg.Pos()).Column,
			},
			Package: pkg,
		})
	}
	return comments
}

func processScope(scope *types.Scope, pkgModel *model.Package) {
	pkg := pkgModel.Package

	for _, name := range scope.Names() {
		obj := scope.Lookup(name)

		pos := pkg.Fset.Position(obj.Pos())
		doc := astx.TypeDoc(pkg.Package, obj) // Get the documentation for the type

		node := model.TypeNode{
			Package: pkgModel,
			Position: model.Position{
				Filename: pos.Filename,
				Line:     pos.Line,
				Column:   pos.Column,
			},
			Doc:         doc,
			Annotations: ParseAnnotations(doc),
		}

		switch t := obj.(type) {
		case *types.Func:
			pkgModel.Functions = append(pkgModel.Functions, buildFunction(pkg, t, &node))
		case *types.Var:
			if !t.Exported() {
				continue
			}
			pkgModel.Vars = append(pkgModel.Vars, &model.PackageVar{
				TypeNode: node,
				Name:     t.Name(),
				Type: &model.TypeDefinition{
					Spec: model.NewTypeSpec(astx.FQNFromGoType(t.Type()), t.Type()),
				},
				VarType: t.Type(),
			})
		case *types.TypeName:
			if !t.Exported() {
				continue
			}
			spec := model.NewTypeSpec(astx.FQNFromGoType(obj.Type()), obj.Type())
			spec.Object = t

			tp := &model.Type{
				TypeNode: &node,
				Spec:     spec,
				Name:     obj.Name(),
			}
			pkgModel.Types = append(pkgModel.Types, tp)

			if named, ok := t.Type().(*types.Named); ok {
				switch ut := named.Underlying().(type) {
				case *types.Interface:
					buildInterface(pkg, tp, ut)
				case *types.Struct:
					s := buildStruct(pkg, tp, ut)
					// Collect methods defined on the named type (pointer and value receivers)
					for method := range named.Methods() {
						s.Methods = append(s.Methods, buildFunction(pkg, method, &model.TypeNode{
							Position: model.Position{
								Filename: pkg.Fset.Position(method.Pos()).Filename,
								Line:     pkg.Fset.Position(method.Pos()).Line,
								Column:   pkg.Fset.Position(method.Pos()).Column,
							},
						}))
					}
				}
			}
		}
	}

	for child := range scope.Children() {
		processScope(child, pkgModel)
	}
}

func buildMethods(pkg *decorator.Package, methods iter.Seq[*types.Func]) []*model.Function {
	var result []*model.Function
	for method := range methods {
		result = append(result, buildFunction(pkg, method, &model.TypeNode{}))
	}
	return result
}

func buildInterface(pkg *decorator.Package, tp *model.Type, iface *types.Interface) *model.Interface {
	i := &model.Interface{
		Interface: iface,
		Methods:   buildMethods(pkg, iface.Methods()),
	}
	tp.Interface = i // Link back to the interface from the type
	return i
}

func buildStruct(pkg *decorator.Package, tp *model.Type, st *types.Struct) *model.Struct {
	s := &model.Struct{
		Struct: st,
	}

	tp.Struct = s // Link back to the struct from the type

	for i := 0; i < st.NumFields(); i++ {
		field := st.Field(i)
		if field.Exported() {
			v := buildVar(pkg, field)
			if tag := st.Tag(i); tag != "" {
				v.Tags = ParseTags(tag)
			}
			s.Fields = append(s.Fields, v)
		}
	}

	return s
}

func buildVar(pkg *decorator.Package, v *types.Var) *model.Var {
	doc := astx.TypeDoc(pkg.Package, v) // Get the documentation for the variable
	t := v.Type()

	return &model.Var{
		Name:        v.Name(),
		Doc:         doc,
		Annotations: ParseAnnotations(doc),
		Type: &model.TypeDefinition{
			Spec: model.NewTypeSpec(astx.FQNFromGoType(t), t),
		},
		Embedded: v.Embedded(),
	}
}

func buildFunction(pkg *decorator.Package, obj *types.Func, node *model.TypeNode) *model.Function {
	signature := obj.Signature()
	params := signature.Params().Len()
	results := signature.Results().Len()

	f := &model.Function{
		TypeNode: *node,
		Name:     obj.Name(),
	}

	if recv := signature.Recv(); recv != nil {
		f.Receiver = buildVar(pkg, recv)
	}

	for i := 0; i < params; i++ {
		f.Args = append(f.Args, buildVar(pkg, signature.Params().At(i)))
	}
	unnamed := 0
	for i := 0; i < results; i++ {
		v := buildVar(pkg, signature.Results().At(i))
		if v.Name == "" {
			unnamed++
			v.FallbackName = fmt.Sprintf("out%d", unnamed)
		}
		f.Results = append(f.Results, v)
	}

	return f
}
