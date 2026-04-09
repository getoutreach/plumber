// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements Go source file scanning and AST inspection to extract types, functions, and annotations into the query model.

// Package inspect provides utilities for scanning Go source files and extracting type and annotation information into the plumber query model.
package inspect

import (
	"fmt"
	"go/ast"
	"go/types"
	"io/fs"
	"iter"
	"path"
	"path/filepath"
	"strings"

	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/query/model"
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
		filepath.WalkDir(arg, walk)
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

		for _, file := range pkg.Package.Syntax {
			pm.Comments = append(pm.Comments, processComments(pkg, file)...)
		}

		processScope(pkg.Package.Types.Scope(), pm)
		pkgs = append(pkgs, pm)
	}
	return pkgs, nil
}

func processComments(pkg *decorator.Package, f *ast.File) []*model.CommentGroup {
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
				Filename: pkg.Fset.Position(cg.Pos()).Filename,
				Line:     pkg.Fset.Position(cg.Pos()).Line,
				Column:   pkg.Fset.Position(cg.Pos()).Column,
			},
		})
	}

	// ast.Inspect(f, func(node ast.Node) bool {
	// 	switch n := node.(type) {
	// 	case *ast.CommentGroup:
	// 		comments = append(comments, &model.CommentGroup{
	// 			Position: model.Position{
	// 				Filename: f.Name.Name,
	// 				Line:     int(n.Pos()),
	// 			},
	// 			Doc:         n.Text(),
	// 			Annotations: ParseAnnotations(n.Text()),
	// 		})
	// 		return false
	// 	// case *ast.GenDecl:
	// 	// 	if n.Doc != nil {
	// 	// 		comments = append(comments, &model.CommentGroup{
	// 	// 			Position: model.Position{
	// 	// 				Filename: f.Name.Name,
	// 	// 				Line:     int(n.Pos()),
	// 	// 			},
	// 	// 			Doc:         n.Doc.Text(),
	// 	// 			Annotations: ParseAnnotations(n.Doc.Text()),
	// 	// 		})
	// 	// 	}
	// 	// 	return false
	// 	default:
	// 		return true
	// 	}
	// })
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
			pkgModel.Functions = append(pkgModel.Functions, buildFunction(pkg, t, node))
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
						s.Methods = append(s.Methods, buildFunction(pkg, method, model.TypeNode{
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
		result = append(result, buildFunction(pkg, method, model.TypeNode{}))
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
	}
}

func buildFunction(pkg *decorator.Package, obj *types.Func, node model.TypeNode) *model.Function {
	signature := obj.Signature()
	params := signature.Params().Len()
	results := signature.Results().Len()

	f := &model.Function{
		TypeNode: node,
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
