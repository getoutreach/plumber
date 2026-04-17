// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements DST-based template rendering and AST manipulation for generating container resolver code.

// Package templates provides DST template rendering utilities for generating container resolver code in the discovery system.
package templates

import (
	"embed"
	"go/parser"
	"go/token"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/dave/dst/dstutil"
	"github.com/getoutreach/plumber/internal/astx"
	"github.com/getoutreach/plumber/internal/discovery/contract"
)

// fixtureFS is an embedded filesystem containing the template for the container resolver code.
//
//go:embed fixtures/*.go
var fixtureFS embed.FS

func ContainerResolver(visitors ...astx.Visitor) *dst.File {
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

	astx.Walk(decorated, append([]astx.Visitor{}, visitors...)...)

	return decorated
}

func SelectorExprNameReplace(mapping map[string]string) astx.Visitor {
	v := &astx.RecursiveVisitor{}
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

func IdentReplace(mapping map[string]any) astx.Visitor {
	v := &astx.RecursiveVisitor{}
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

func TypeDefinition(param contract.ParameterInfo) func(c *dstutil.Cursor) {
	return func(c *dstutil.Cursor) {
		c.Replace(astx.ToTypeDefinition(param.TypeInfo.Type))
	}
}
