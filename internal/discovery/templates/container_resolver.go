package templates

import (
	"embed"
	"go/parser"
	"go/token"
	"go/types"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/dave/dst/dstutil"
	"github.com/getoutreach/plumber/internal/discovery/contract"
)

//go:embed fixtures/*.go
var fixtureFS embed.FS

type Visitor interface {
	Pre(c *dstutil.Cursor) bool
	Post(c *dstutil.Cursor) bool
}

type RecursiveVisitor struct {
	PreFunc  func(c *dstutil.Cursor) bool
	PostFunc func(c *dstutil.Cursor) bool
}

func (v *RecursiveVisitor) Pre(c *dstutil.Cursor) bool {
	if v.PreFunc != nil {
		return v.PreFunc(c)
	}
	return true
}

func (v *RecursiveVisitor) Post(c *dstutil.Cursor) bool {
	if v.PostFunc != nil {
		return v.PostFunc(c)
	}
	return true
}

type functionBodyExtractor struct {
	RecursiveVisitor
	block *dst.BlockStmt
}

func (v *functionBodyExtractor) Pre(c *dstutil.Cursor) bool {
	ret, ok := c.Node().(*dst.FuncDecl)
	if ok {
		v.block = ret.Body
		return true
	}
	return true
}

func walk(node dst.Node, visitors ...Visitor) dst.Node {
	dstutil.Apply(
		node,
		func(c *dstutil.Cursor) bool {
			for _, visitor := range visitors {
				if !visitor.Pre(c) {
					return false
				}
			}
			return true
		},
		func(c *dstutil.Cursor) bool {
			for _, visitor := range visitors {
				if !visitor.Post(c) {
					return false
				}
			}
			return true
		},
	)
	return node
}

func ContainerResolver(visitors ...Visitor) *dst.File {
	// bodyExtractor := functionBodyExtractor{}
	// &bodyExtractor

	template, err := fixtureFS.ReadFile("fixtures/container_resolver_resolve.go")
	if err != nil {
		panic(err)
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "templates", template, parser.ParseComments)
	if err != nil {
		panic(err)
	}

	decorated, err := decorator.DecorateFile(fset, f)
	if err != nil {
		panic(err)
	}

	walk(decorated, append([]Visitor{}, visitors...)...)

	return decorated
}

func Walk(node dst.Node, visitors ...Visitor) dst.Node {
	walk(node, visitors...)
	return node
}

func SelectorExprNameReplace(mapping map[string]string) Visitor {
	v := &RecursiveVisitor{}
	v.PreFunc = func(c *dstutil.Cursor) bool {
		sel, ok := c.Node().(*dst.SelectorExpr)
		if ok {
			name := sel.Sel.Name
			if newName, exists := mapping[name]; exists {
				sel.Sel.Name = newName
			}
		}
		return true
	}
	return v
}

func IdentReplace(mapping map[string]any) Visitor {
	v := &RecursiveVisitor{}
	v.PreFunc = func(c *dstutil.Cursor) bool {
		ident, ok := c.Node().(*dst.Ident)
		if ok {
			name := ident.Name
			if replacement, exists := mapping[name]; exists {
				switch replacement := replacement.(type) {
				case string:
					ident.Name = replacement
				case func(c *dstutil.Cursor):
					replacement(c)
				}
			}
			return false
		}
		return true
	}
	return v
}

func FuncBodyStmts(node dst.Node) []dst.Stmt {
	extractor := &functionBodyExtractor{}
	walk(node, extractor)
	return extractor.block.List
}

func FuncDeclaration(node dst.Node, name string) *dst.FuncDecl {
	var funcDeclaration *dst.FuncDecl
	visitor := &RecursiveVisitor{
		PreFunc: func(c *dstutil.Cursor) bool {
			funcDecl, ok := c.Node().(*dst.FuncDecl)
			if ok && funcDecl.Name.Name == name {
				funcDeclaration = funcDecl
				return false // Stop walking once we've found the function declaration
			}
			return true
		},
	}
	walk(node, visitor)
	return funcDeclaration
}

func FindNodes(node dst.Node, predicate func(dst.Node) (match, recurse bool)) []dst.Node {
	var selected []dst.Node
	visitor := &RecursiveVisitor{
		PreFunc: func(c *dstutil.Cursor) bool {
			match, recurse := predicate(c.Node())
			if match {
				selected = append(selected, c.Node())
			}
			return recurse
		},
	}
	walk(node, visitor)
	return selected
}

func FindNode(node dst.Node, predicate func(dst.Node) (match, recurse bool)) dst.Node {
	nodes := FindNodes(node, predicate)
	if len(nodes) > 0 {
		return nodes[0]
	}
	return nil
}

func IsFuncCallTo(node dst.Node, funcName string) bool {
	if call, ok := node.(*dst.CallExpr); ok {
		if sel, ok := call.Fun.(*dst.SelectorExpr); ok {
			if sel.Sel.Name == funcName {
				return true
			}
		}
	}
	return false
}

func MatchOnly(b bool) (match, recurse bool) {
	if b {
		return true, false
	}
	return false, true
}

func MatchType[T any](node dst.Node, predicates ...func(T) (match bool)) (match, recurse bool) {
	if n, ok := node.(T); ok {
		for _, predicate := range predicates {
			if !predicate(n) {
				return false, true
			}
		}
		return true, false
	}
	return false, true
}

func Matcher(match, recurse bool) func() (match, recurse bool) {
	return func() (match, recurse bool) {
		return match, recurse
	}
}

func StopRecurseOnMatch(pred func() (match, recurse bool)) func() (match, recurse bool) {
	return func() (match, recurse bool) {
		match, _ = pred()
		if match {
			return false, false
		}
		return false, true
	}
}

func MatchAny(predicates ...func() (match, recurse bool)) (match, recurse bool) {
	for _, predicate := range predicates {
		if match, recurse := predicate(); match {
			if !match && !recurse {
				return false, false
			}
			if match {
				return true, recurse
			}
		}
	}
	return false, true
}

func FindCallbackBody(call dst.Node, argument int) dst.Node {
	if call, ok := call.(*dst.CallExpr); ok {
		if argument >= len(call.Args) {
			return nil
		}
		arg := call.Args[argument]
		if funcLit, ok := arg.(*dst.FuncLit); ok {
			return funcLit.Body
		}
	}
	return nil
}

func TypeDefinition(param contract.ParameterInfo) func(c *dstutil.Cursor) {
	return func(c *dstutil.Cursor) {
		c.Replace(ToTypeDefinition(param.TypeInfo.Type))
	}
}

func ToTypeDefinition(t types.Type) dst.Expr {
	switch t := t.(type) {
	case *types.Named:
		return &dst.Ident{Name: t.Obj().Name(), Path: t.Obj().Pkg().Path()}
	case *types.Pointer:
		return &dst.StarExpr{X: ToTypeDefinition(t.Elem())}
	case *types.Slice:
		return &dst.ArrayType{Elt: ToTypeDefinition(t.Elem())}
	case *types.Map:
		return &dst.MapType{
			Key:   ToTypeDefinition(t.Key()),
			Value: ToTypeDefinition(t.Elem()),
		}
	default:
		return &dst.Ident{Name: t.String()}
	}
}

// func IdentReplace(mapping map[string]string) RecursiveVisitor {
// 	v := RecursiveVisitor{}
// 	v.VisitFunc = func(n ast.Node) ast.Visitor {
// 		ident, ok := n.(*ast.Ident)
// 		if ok {
// 			name := ident.Name
// 			if newName, exists := mapping[name]; exists {
// 				ident.Name = newName
// 			}
// 		}
// 		return v
// 	}
// 	return v
// }
