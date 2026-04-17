// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Types to ast expression conversion
package discovery

import (
	"fmt"
	"go/types"
	"strings"

	"github.com/dave/dst"
)

// TypeToExpr converts a go/types.Type to a dst.Expr suitable for AST manipulation.
// This is useful when you need to inject fields with types discovered through type checking.
// nolint: gocyclo,funlen //Why: The function is a straightforward type switch
func TypeToExpr(typ types.Type, qualifier types.Qualifier) dst.Expr {
	if typ == nil {
		return &dst.Ident{Name: "interface{}"}
	}

	switch t := typ.(type) {
	case *types.Basic:
		return &dst.Ident{Name: t.Name()}

	case *types.Pointer:
		return &dst.StarExpr{
			X: TypeToExpr(t.Elem(), qualifier),
		}

	case *types.Slice:
		return &dst.ArrayType{
			Elt: TypeToExpr(t.Elem(), qualifier),
		}

	case *types.Array:
		return &dst.ArrayType{
			Len: &dst.BasicLit{Value: fmt.Sprintf("%d", t.Len())},
			Elt: TypeToExpr(t.Elem(), qualifier),
		}

	case *types.Map:
		return &dst.MapType{
			Key:   TypeToExpr(t.Key(), qualifier),
			Value: TypeToExpr(t.Elem(), qualifier),
		}

	case *types.Chan:
		chanType := &dst.ChanType{
			Value: TypeToExpr(t.Elem(), qualifier),
		}
		switch t.Dir() {
		case types.SendRecv:
			chanType.Dir = dst.SEND | dst.RECV
		case types.SendOnly:
			chanType.Dir = dst.SEND
		case types.RecvOnly:
			chanType.Dir = dst.RECV
		}
		return chanType

	case *types.Named:
		obj := t.Obj()
		pkg := obj.Pkg()

		// Handle type parameters
		var typeArgs []dst.Expr
		if t.TypeArgs() != nil {
			for i := 0; i < t.TypeArgs().Len(); i++ {
				typeArgs = append(typeArgs, TypeToExpr(t.TypeArgs().At(i), qualifier))
			}
		}

		var expr dst.Expr
		if pkg == nil || (qualifier != nil && qualifier(pkg) == "") {
			// Unqualified (same package or universe scope)
			expr = &dst.Ident{Name: obj.Name()}
		} else {
			// Qualified with package
			pkgName := pkg.Name()
			if qualifier != nil {
				if q := qualifier(pkg); q != "" {
					pkgName = q
				}
			}
			expr = &dst.SelectorExpr{
				X:   &dst.Ident{Name: pkgName},
				Sel: &dst.Ident{Name: obj.Name()},
			}
		}

		// Add type arguments if present (generics)
		if len(typeArgs) == 1 {
			expr = &dst.IndexExpr{
				X:     expr,
				Index: typeArgs[0],
			}
		} else if len(typeArgs) > 1 {
			expr = &dst.IndexListExpr{
				X:       expr,
				Indices: typeArgs,
			}
		}

		return expr

	case *types.Interface:
		// For interface{}, return empty interface literal
		if t.NumMethods() == 0 && t.NumEmbeddeds() == 0 {
			return &dst.InterfaceType{
				Methods: &dst.FieldList{},
			}
		}
		// For complex interfaces, fall back to string representation
		return &dst.Ident{Name: types.TypeString(t, qualifier)}

	case *types.Struct:
		// Inline struct type
		fields := &dst.FieldList{}
		for i := 0; i < t.NumFields(); i++ {
			f := t.Field(i)
			field := &dst.Field{
				Type: TypeToExpr(f.Type(), qualifier),
			}
			if !f.Embedded() {
				field.Names = []*dst.Ident{{Name: f.Name()}}
			}
			if tag := t.Tag(i); tag != "" {
				field.Tag = &dst.BasicLit{Value: "`" + tag + "`"}
			}
			fields.List = append(fields.List, field)
		}
		return &dst.StructType{Fields: fields}

	case *types.Signature:
		return signatureToFuncType(t, qualifier)

	case *types.TypeParam:
		return &dst.Ident{Name: t.Obj().Name()}

	default:
		// Fallback: use string representation
		typeStr := types.TypeString(typ, qualifier)
		// Parse simple cases
		if strings.Contains(typeStr, ".") {
			parts := strings.SplitN(typeStr, ".", 2)
			return &dst.SelectorExpr{
				X:   &dst.Ident{Name: parts[0]},
				Sel: &dst.Ident{Name: parts[1]},
			}
		}
		return &dst.Ident{Name: typeStr}
	}
}

func signatureToFuncType(sig *types.Signature, qualifier types.Qualifier) *dst.FuncType {
	funcType := &dst.FuncType{
		Params:  tupleToFieldList(sig.Params(), qualifier),
		Results: tupleToFieldList(sig.Results(), qualifier),
	}

	// Handle type parameters (generics)
	if tparams := sig.TypeParams(); tparams != nil && tparams.Len() > 0 {
		typeParams := &dst.FieldList{}
		for i := 0; i < tparams.Len(); i++ {
			tp := tparams.At(i)
			typeParams.List = append(typeParams.List, &dst.Field{
				Names: []*dst.Ident{{Name: tp.Obj().Name()}},
				Type:  TypeToExpr(tp.Constraint(), qualifier),
			})
		}
		funcType.TypeParams = typeParams
	}

	return funcType
}

func tupleToFieldList(tuple *types.Tuple, qualifier types.Qualifier) *dst.FieldList {
	if tuple == nil || tuple.Len() == 0 {
		return &dst.FieldList{}
	}

	fields := &dst.FieldList{}
	for i := 0; i < tuple.Len(); i++ {
		v := tuple.At(i)
		field := &dst.Field{
			Type: TypeToExpr(v.Type(), qualifier),
		}
		if name := v.Name(); name != "" {
			field.Names = []*dst.Ident{{Name: name}}
		}
		fields.List = append(fields.List, field)
	}
	return fields
}
