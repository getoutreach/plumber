package astx

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/printer"
	"go/token"
	"go/types"
	"strconv"
	"strings"
)

// typeToAST converts a types.Type into its ast.Expr representation.
// Named types with a package are represented as a SelectorExpr where
// the X ident carries the quoted package path, e.g.
//
//	"github.com/google/uuid".UUID
func typeToAST(t types.Type) ast.Expr {
	switch t := t.(type) {
	case *types.Basic:
		return &ast.Ident{Name: t.Name()}

	case *types.Named:
		obj := t.Obj()
		if pkg := obj.Pkg(); pkg != nil {
			return &ast.SelectorExpr{
				// Store the full package path as a quoted string inside the Ident
				// so it serialises as "pkg/path".TypeName
				X:   &ast.Ident{Name: strconv.Quote(pkg.Path())},
				Sel: &ast.Ident{Name: obj.Name()},
			}
		}
		return &ast.Ident{Name: obj.Name()}

	case *types.Pointer:
		return &ast.StarExpr{X: typeToAST(t.Elem())}

	case *types.Slice:
		return &ast.ArrayType{Elt: typeToAST(t.Elem())}

	case *types.Array:
		return &ast.ArrayType{
			Len: &ast.BasicLit{Kind: token.INT, Value: strconv.FormatInt(t.Len(), 10)},
			Elt: typeToAST(t.Elem()),
		}

	case *types.Map:
		return &ast.MapType{
			Key:   typeToAST(t.Key()),
			Value: typeToAST(t.Elem()),
		}

	case *types.Chan:
		dir := ast.SEND | ast.RECV
		switch t.Dir() {
		case types.SendOnly:
			dir = ast.SEND
		case types.RecvOnly:
			dir = ast.RECV
		}
		return &ast.ChanType{Dir: dir, Value: typeToAST(t.Elem())}

	case *types.Interface:
		return &ast.InterfaceType{Methods: &ast.FieldList{}}

	case *types.Struct:
		return &ast.StructType{Fields: &ast.FieldList{}}

	case *types.Signature:
		return &ast.FuncType{}

	case *types.TypeParam:
		return &ast.Ident{Name: t.Obj().Name()}

	default:
		// Fallback: use the types package string representation
		return &ast.Ident{Name: t.String()}
	}
}

type FQN struct {
	Expression ast.Expr
}

func (f *FQN) IsStandard() bool {
	return StandardType(f.String())
}

func (f *FQN) IsPackageLess() bool {
	return IsPackageLess(f.String())
}

func StandardType(name string) bool {
	return !strings.Contains(name, `/`)
}

func IsPackageLess(name string) bool {
	return !strings.Contains(name, `.`)
}

func (f *FQN) Unquote() string {
	return strings.ReplaceAll(f.String(), `"`, ``)
}

// FQNFromGoType returns the fully qualified name of a Go type.
// Package paths are quoted and separated from the type name by a dot, e.g.:
//
//	*"github.com/google/uuid".UUID
//	*[]"net/http".Dir
func FQNFromGoType(t types.Type) *FQN {
	expr := typeToAST(t)
	return &FQN{Expression: expr}
}

func ParseFQN(s string) (*FQN, error) {
	p := &fqnParser{s: s}
	expr, err := p.parse()
	if err != nil {
		return nil, err
	}
	if p.pos != len(p.s) {
		return nil, fmt.Errorf("unexpected trailing input: %q", p.s[p.pos:])
	}
	return &FQN{Expression: expr}, nil
}

// fqnParser is a small recursive-descent parser for the FQN string format
// produced by FQN.String / FQNFromGoType.
type fqnParser struct {
	s   string
	pos int
}

func (p *fqnParser) peek() byte {
	if p.pos >= len(p.s) {
		return 0
	}
	return p.s[p.pos]
}

func (p *fqnParser) has(prefix string) bool {
	return strings.HasPrefix(p.s[p.pos:], prefix)
}

func (p *fqnParser) parse() (ast.Expr, error) {
	switch {
	case p.peek() == '*':
		p.pos++
		inner, err := p.parse()
		if err != nil {
			return nil, err
		}
		return &ast.StarExpr{X: inner}, nil

	case p.has("[]"):
		p.pos += 2
		inner, err := p.parse()
		if err != nil {
			return nil, err
		}
		return &ast.ArrayType{Elt: inner}, nil

	case p.has("map["):
		p.pos += 4
		key, err := p.parse()
		if err != nil {
			return nil, fmt.Errorf("map key: %w", err)
		}
		if p.peek() != ']' {
			return nil, fmt.Errorf("expected ']' after map key, got %q", p.s[p.pos:])
		}
		p.pos++ // consume ']'
		val, err := p.parse()
		if err != nil {
			return nil, fmt.Errorf("map value: %w", err)
		}
		return &ast.MapType{Key: key, Value: val}, nil

	case p.peek() == '[':
		// Fixed-size array: [N]T
		p.pos++ // consume '['
		start := p.pos
		for p.pos < len(p.s) && p.s[p.pos] >= '0' && p.s[p.pos] <= '9' {
			p.pos++
		}
		if p.peek() != ']' {
			return nil, fmt.Errorf("expected ']' after array length")
		}
		length := p.s[start:p.pos]
		p.pos++ // consume ']'
		elem, err := p.parse()
		if err != nil {
			return nil, err
		}
		return &ast.ArrayType{
			Len: &ast.BasicLit{Kind: token.INT, Value: length},
			Elt: elem,
		}, nil

	case p.has("<-chan "):
		p.pos += len("<-chan ")
		inner, err := p.parse()
		if err != nil {
			return nil, err
		}
		return &ast.ChanType{Dir: ast.RECV, Value: inner}, nil

	case p.has("chan<- "):
		p.pos += len("chan<- ")
		inner, err := p.parse()
		if err != nil {
			return nil, err
		}
		return &ast.ChanType{Dir: ast.SEND, Value: inner}, nil

	case p.has("chan "):
		p.pos += len("chan ")
		inner, err := p.parse()
		if err != nil {
			return nil, err
		}
		return &ast.ChanType{Dir: ast.SEND | ast.RECV, Value: inner}, nil

	case p.peek() == '"':
		// Named type with package: "pkg/path".TypeName
		// Package paths never contain backslash-escapes so a simple scan works.
		end := strings.Index(p.s[p.pos+1:], `"`)
		if end < 0 {
			return nil, fmt.Errorf("unterminated package path at %q", p.s[p.pos:])
		}
		pkg := p.s[p.pos : p.pos+end+2] // includes both surrounding quote chars
		p.pos += end + 2
		if p.peek() != '.' {
			return nil, fmt.Errorf("expected '.' after package path, got %q", p.s[p.pos:])
		}
		p.pos++ // consume '.'
		name := p.ident()
		if name == "" {
			return nil, fmt.Errorf("expected type name after '.'")
		}
		return &ast.SelectorExpr{
			X:   &ast.Ident{Name: pkg},
			Sel: &ast.Ident{Name: name},
		}, nil

	case p.has("interface{}"):
		p.pos += len("interface{}")
		return &ast.InterfaceType{Methods: &ast.FieldList{}}, nil

	case p.has("struct{}"):
		p.pos += len("struct{}")
		return &ast.StructType{Fields: &ast.FieldList{}}, nil

	case p.has("func()"):
		p.pos += len("func()")
		return &ast.FuncType{}, nil

	default:
		name := p.ident()
		if name == "" {
			if p.pos < len(p.s) {
				return nil, fmt.Errorf("unexpected character %q at position %d", p.s[p.pos], p.pos)
			}
			return nil, fmt.Errorf("unexpected end of input")
		}
		return &ast.Ident{Name: name}, nil
	}
}

// ident reads a Go identifier (letters, digits, underscore) from the current position.
func (p *fqnParser) ident() string {
	start := p.pos
	for p.pos < len(p.s) {
		c := p.s[p.pos]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			break
		}
		p.pos++
	}
	return p.s[start:p.pos]
}

func (f *FQN) String() string {
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, token.NewFileSet(), f.Expression); err != nil {
		return ""
	}
	return buf.String()
}

// WalkPackages walks the FQN expression tree and calls fn for every remote-package
// type reference — an *ast.SelectorExpr whose X is an *ast.Ident carrying a
// quoted import path (e.g. "github.com/google/uuid").
//
// fn receives the unquoted package path and the type name. The value it returns
// replaces the original node; return nil to leave the node unchanged.
func (f *FQN) WalkPackages(fn func(pkgPath, typeName string) (string, bool)) {
	f.Expression = walkExpr(f.Expression, fn)
}

// walkExpr recursively walks an ast.Expr, replacing remote-package SelectorExprs
// via fn and returning the (possibly replaced) expression.
func walkExpr(expr ast.Expr, fn func(pkgPath, typeName string) (string, bool)) ast.Expr {
	switch e := expr.(type) {
	case *ast.StarExpr:
		e.X = walkExpr(e.X, fn)
	case *ast.ArrayType:
		e.Elt = walkExpr(e.Elt, fn)
	case *ast.MapType:
		e.Key = walkExpr(e.Key, fn)
		e.Value = walkExpr(e.Value, fn)
	case *ast.ChanType:
		e.Value = walkExpr(e.Value, fn)
	case *ast.SelectorExpr:
		if ident, ok := e.X.(*ast.Ident); ok {
			empty := false
			if pkgPath, err := strconv.Unquote(ident.Name); err == nil { // && strings.Contains(pkgPath, "/")
				if replacement, ok := fn(pkgPath, e.Sel.Name); ok {
					ident.Name = replacement
					if replacement == "" {
						empty = true
					}
				}
			}
			if empty {
				return &ast.Ident{Name: e.Sel.Name} // package-less reference
			}
		}
	}
	return expr
}
