// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides general-purpose DST (Decorated Syntax Tree) walking and
// node-searching utilities built on top of dstutil.Apply, extracted from the discovery templates
// package for reuse across plumber subsystems.

package astx

import (
	"go/types"

	"github.com/dave/dst"
	"github.com/dave/dst/dstutil"
)

// Visitor is an interface that defines methods for pre-order and post-order traversal of DST nodes during AST manipulation.
type Visitor interface {
	Pre(c *dstutil.Cursor) bool
	Post(c *dstutil.Cursor) bool
}

// RecursiveVisitor is a helper struct that implements the Visitor interface with customizable pre-order and post-order functions.
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

// functionBodyExtractor is a Visitor that extracts the body of a function declaration during AST traversal.
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

// walk applies visitors to a DST tree using dstutil.Apply.
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

// Walk applies the given visitors to the DST tree rooted at node using pre-order and post-order traversal.
func Walk(node dst.Node, visitors ...Visitor) dst.Node {
	walk(node, visitors...)
	return node
}

// FuncBodyStmts returns the statements in the body of the first function declaration found in the given DST node.
func FuncBodyStmts(node dst.Node) []dst.Stmt {
	extractor := &functionBodyExtractor{}
	walk(node, extractor)
	return extractor.block.List
}

// FuncDeclaration finds and returns the first function declaration with the given name in the DST tree.
func FuncDeclaration(node dst.Node, name string) *dst.FuncDecl {
	var funcDeclaration *dst.FuncDecl
	visitor := &RecursiveVisitor{
		PreFunc: func(c *dstutil.Cursor) bool {
			funcDecl, ok := c.Node().(*dst.FuncDecl)
			if ok && funcDecl.Name.Name == name {
				funcDeclaration = funcDecl
				return false
			}
			return true
		},
	}
	walk(node, visitor)
	return funcDeclaration
}

// FindNodes walks the DST tree and returns all nodes matching the predicate.
// The predicate returns (match, recurse) — match indicates the node should be collected,
// recurse indicates whether to continue walking into children.
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

// FindNode walks the DST tree and returns the first node matching the predicate.
func FindNode(node dst.Node, predicate func(dst.Node) (match, recurse bool)) dst.Node {
	nodes := FindNodes(node, predicate)
	if len(nodes) > 0 {
		return nodes[0]
	}
	return nil
}

// IsFuncCallTo checks whether the given node is a method call expression to the named function.
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

// MatchOnly converts a boolean match result into the (match, recurse) pair expected by FindNodes.
// When matched, recursion stops; when not matched, recursion continues.
func MatchOnly(b bool) (match, recurse bool) {
	if b {
		return true, false
	}
	return false, true
}

// MatchType is a generic predicate helper for FindNodes that matches nodes of a specific DST type
// and optionally applies additional predicate functions on the typed node.
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

// Matcher wraps a (match, recurse) pair into a closure, useful for composing predicate functions.
func Matcher(match, recurse bool) func() (match, recurse bool) {
	return func() (match, recurse bool) {
		return match, recurse
	}
}

// StopRecurseOnMatch wraps a predicate to stop recursion when a match is found.
func StopRecurseOnMatch(pred func() (match, recurse bool)) func() (match, recurse bool) {
	return func() (match, recurse bool) {
		match, _ = pred()
		if match {
			return false, false
		}
		return false, true
	}
}

// MatchAny returns true if any of the given predicates match, short-circuiting on the first match.
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

// FindCallbackBody returns the body of a function literal passed as the given argument index
// in a call expression, or nil if the node is not a call or the argument is not a function literal.
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

// ToTypeDefinition converts a go/types.Type into its corresponding DST expression node,
// handling named types, pointers, slices, and maps.
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
