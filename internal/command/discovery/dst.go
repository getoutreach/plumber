// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides DST expression helpers for constructing provider path expressions
// and converting DST expressions to strings.

// Package discovery implements automatic dependency graph discovery from Go source code using AST analysis.
package discovery

import (
	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/command/discovery/contract"
	"github.com/samber/lo"
)

func providerPathExpr(p *contract.ContainerProvider, currentContainerName string) dst.Expr {
	if currentContainerName == p.ContainerName {
		return &dst.SelectorExpr{
			X: &dst.Ident{
				Name: "c",
			},
			Sel: dst.NewIdent(p.Provider.Name),
		}
	}

	return &dst.SelectorExpr{
		X: &dst.SelectorExpr{
			X: &dst.Ident{
				Name: "a",
			},
			Sel: dst.NewIdent(p.ContainerName),
		},
		Sel: &dst.Ident{
			Name: p.Provider.Name,
		},
	}
}

func newLinedArguments(mappedArgs []dst.Expr) []dst.Expr {
	return lo.Map(mappedArgs, func(arg dst.Expr, _ int) dst.Expr {
		arg.Decorations().After = dst.NewLine
		arg.Decorations().Before = dst.NewLine
		return arg
	})
}

func ExprToString(expr dst.Expr) string {
	switch e := expr.(type) {
	case *dst.SelectorExpr:
		return ExprToString(e.X) + "." + e.Sel.Name
	case *dst.Ident:
		return e.Name
	case *dst.UnaryExpr:
		return e.Op.String() + ExprToString(e.X)
	default:
		return ""
	}
}
