package astx

import (
	"go/ast"
	"go/token"

	"golang.org/x/tools/go/packages"
)

func TypeDoc(pkg *packages.Package, typeName interface{ Pos() token.Pos }) string {
	pos := typeName.Pos()
	for _, file := range pkg.Syntax {
		for _, decl := range file.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range genDecl.Specs {
				if typeSpec, ok := spec.(*ast.TypeSpec); ok {
					if typeSpec.Name.Pos() == pos {
						// typeSpec.Doc covers "type Foo interface { ... }" with its own comment
						if typeSpec.Doc != nil {
							return typeSpec.Doc.Text()
						}
						// genDecl.Doc covers the group-level comment (e.g. a single-type block)
						if genDecl.Doc != nil {
							return genDecl.Doc.Text()
						}
						return ""
					}
					// Check if the position belongs to a field inside this struct type.
					if structType, ok := typeSpec.Type.(*ast.StructType); ok {
						if doc := fieldDoc(structType, pos); doc != "" {
							return doc
						}
					}
				}
				if valueSpec, ok := spec.(*ast.ValueSpec); ok {
					for _, name := range valueSpec.Names {
						if name.Pos() == pos {
							if valueSpec.Doc != nil {
								return valueSpec.Doc.Text()
							}
							if genDecl.Doc != nil {
								return genDecl.Doc.Text()
							}
							return ""
						}
					}
				}
			}
		}
	}
	return ""
}

// fieldDoc searches a struct's field list for a field whose name (or embedded
// type identifier) is located at pos and returns its doc comment.  Inline
// comments (field.Comment) are used as a fallback when no leading doc block
// is present.
func fieldDoc(st *ast.StructType, pos token.Pos) string {
	for _, field := range st.Fields.List {
		if len(field.Names) == 0 {
			// Embedded (anonymous) field: the implicit field name is the
			// unqualified type name, whose position we must resolve from the
			// type expression (stripping any leading "*" or qualifier).
			if embeddedFieldNamePos(field.Type) == pos {
				return fieldComment(field)
			}
		} else {
			for _, name := range field.Names {
				if name.Pos() == pos {
					return fieldComment(field)
				}
			}
		}
	}
	return ""
}

// fieldComment returns the documentation string for a struct field, preferring
// the leading doc block and falling back to the trailing line comment.
func fieldComment(field *ast.Field) string {
	if field.Doc != nil {
		return field.Doc.Text()
	}
	if field.Comment != nil {
		return field.Comment.Text()
	}
	return ""
}

// embeddedFieldNamePos resolves the position of the bare type identifier for
// an embedded (anonymous) struct field, handling pointer indirection and
// qualified names (e.g. pkg.Type).
func embeddedFieldNamePos(expr ast.Expr) token.Pos {
	switch t := expr.(type) {
	case *ast.StarExpr:
		return embeddedFieldNamePos(t.X)
	case *ast.SelectorExpr:
		return t.Sel.Pos()
	case *ast.Ident:
		return t.Pos()
	}
	return token.NoPos
}
