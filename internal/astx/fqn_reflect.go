// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements conversion from reflect.Type values into
// the FQN representation, mirroring FQNFromGoType for runtime-only type info.

package astx

import (
	"go/ast"
	"go/token"
	"reflect"
	"strconv"
)

// reflectTypeToAST converts a reflect.Type into its ast.Expr representation
// using the same conventions as typeToAST: named types with a non-empty
// PkgPath are rendered as SelectorExpr where X is an Ident carrying the
// quoted package path, and predeclared / packageless types are rendered as
// bare Idents.
func reflectTypeToAST(t reflect.Type) ast.Expr {
	if t == nil {
		return &ast.Ident{Name: "nil"}
	}

	// Named types: t.Name() is non-empty. PkgPath() is empty for predeclared
	// types like "string", "int", and for the predeclared "error".
	if name := t.Name(); name != "" {
		if pkgPath := t.PkgPath(); pkgPath != "" {
			return &ast.SelectorExpr{
				X:   &ast.Ident{Name: strconv.Quote(pkgPath)},
				Sel: &ast.Ident{Name: name},
			}
		}
		return &ast.Ident{Name: name}
	}

	// Unnamed composites: recurse on element/key/value types.
	// nolint: exhaustive //Why: we only handle the most common composite kinds here; the default case is there.
	switch t.Kind() {
	case reflect.Pointer:
		return &ast.StarExpr{X: reflectTypeToAST(t.Elem())}

	case reflect.Slice:
		return &ast.ArrayType{Elt: reflectTypeToAST(t.Elem())}

	case reflect.Array:
		return &ast.ArrayType{
			Len: &ast.BasicLit{Kind: token.INT, Value: strconv.FormatInt(int64(t.Len()), 10)},
			Elt: reflectTypeToAST(t.Elem()),
		}

	case reflect.Map:
		return &ast.MapType{
			Key:   reflectTypeToAST(t.Key()),
			Value: reflectTypeToAST(t.Elem()),
		}

	case reflect.Chan:
		dir := ast.SEND | ast.RECV
		// nolint: exhaustive //Why: default already handles bidirectional
		switch t.ChanDir() {
		case reflect.SendDir:
			dir = ast.SEND
		case reflect.RecvDir:
			dir = ast.RECV
		}
		return &ast.ChanType{Dir: dir, Value: reflectTypeToAST(t.Elem())}

	case reflect.Func:
		return &ast.FuncType{}

	case reflect.Interface:
		// Anonymous empty interface renders as `any` for a compact,
		// single-line FQN. Anonymous non-empty interfaces fall back to
		// the runtime string form.
		if t.NumMethod() == 0 {
			return &ast.Ident{Name: "any"}
		}
		return &ast.Ident{Name: t.String()}

	case reflect.Struct:
		// Anonymous structs are rendered via the runtime string form to
		// keep the FQN single-line.
		return &ast.Ident{Name: t.String()}

	default:
		// Fallback to the runtime string representation.
		return &ast.Ident{Name: t.String()}
	}
}

// FQNFromReflectType returns the fully qualified name of a Go type as
// observed via reflection. Named types use their PkgPath() to render the
// quoted-package form, e.g.:
//
//	*"net/url".URL
//	[]"github.com/getoutreach/plumber/query/model".TypeSpec
//
// Predeclared types (string, int, bool, error, ...) are rendered as bare
// identifiers.
func FQNFromReflectType(t reflect.Type) *FQN {
	return &FQN{Expression: reflectTypeToAST(t)}
}
