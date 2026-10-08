// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements structural matching and merging of expression lists
// (call arguments and unkeyed composite literal elements) for inplace merge. It lets new
// arguments rendered by a template blend into an existing initializer such as
// `var s = mapping.NewSchema(mapping.Column(...), ...)` without duplicating or
// rewriting the arguments that are already present.

package shape

import (
	"fmt"
	"strings"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/astx"
)

// mergeExprList merges template expressions into existing ones and returns the updated
// list together with a flag reporting whether anything changed.
//
// Each template expression is matched against a not-yet-matched existing expression:
//  1. exact structural match (exprSig), then
//  2. identity match (exprIdentity) — e.g. the same call target with the same first
//     argument, so an existing entry whose trailing arguments were edited is still
//     recognized as the same entry.
//
// Matched expressions get their doc comment reconciled (preserve-manual rule) and are
// deep-merged recursively. Unmatched expressions are cloned and inserted right after
// the last matched (or inserted) position, so template order is kept and hand-written
// entries stay where they are. When allowInsert is false nothing is inserted.
//
// Unmatched structured expressions (those with an exprIdentity, e.g. calls and typed
// literals) are always inserted — they represent new entries. Unmatched leaf
// expressions (identifiers, selectors, literals) usually mean the user edited a
// positional argument, so at most maxLeafInserts of them are inserted; pass a negative
// value for no limit.
func mergeExprList(
	existing, template []dst.Expr, importMap map[string]string, allowInsert bool, maxLeafInserts int,
) ([]dst.Expr, bool) {
	changed := false
	used := make(map[dst.Expr]bool, len(existing)+len(template))
	lastPos := -1

	for _, tExpr := range template {
		idx := findExprMatch(existing, used, tExpr)
		if idx >= 0 {
			e := existing[idx]
			used[e] = true
			if mergeDocComment(&e.Decorations().Start, tExpr.Decorations().Start) {
				changed = true
			}
			if deepMergeExpr(e, tExpr, importMap) {
				changed = true
			}
			if idx > lastPos {
				lastPos = idx
			}
			continue
		}
		if !allowInsert {
			continue
		}
		if exprIdentity(tExpr) == "" {
			if maxLeafInserts == 0 {
				continue
			}
			maxLeafInserts--
		}

		newExpr := astx.RewriteExpr(dst.Clone(tExpr).(dst.Expr), importMap)
		newExpr.Decorations().Before = dst.NewLine
		newExpr.Decorations().After = dst.NewLine

		pos := lastPos + 1
		existing = append(existing, nil)
		copy(existing[pos+1:], existing[pos:])
		existing[pos] = newExpr

		used[newExpr] = true
		lastPos = pos
		changed = true
	}
	return existing, changed
}

// findExprMatch returns the index of the existing expression matching tExpr, or -1.
func findExprMatch(existing []dst.Expr, used map[dst.Expr]bool, tExpr dst.Expr) int {
	sig := exprSig(tExpr)
	for i, e := range existing {
		if !used[e] && exprSig(e) == sig {
			return i
		}
	}
	id := exprIdentity(tExpr)
	if id == "" {
		return -1
	}
	for i, e := range existing {
		if !used[e] && exprIdentity(e) == id {
			return i
		}
	}
	return -1
}

// exprIdentity returns a coarse identity key used to recognise an existing expression
// as "the same entry" as a template expression even when their details differ. Only
// structured expressions have an identity; for everything else "" is returned so the
// expression is matched exactly or treated as new.
func exprIdentity(expr dst.Expr) string {
	switch e := expr.(type) {
	case *dst.CallExpr:
		// Same target + same first argument, e.g. mapping.Column(mapping.EntityColumn(X), ...).
		first := ""
		if len(e.Args) > 0 {
			first = exprSig(e.Args[0])
		}
		return "call:" + exprSig(e.Fun) + "(" + first
	case *dst.UnaryExpr:
		if inner := exprIdentity(e.X); inner != "" {
			return e.Op.String() + inner
		}
	case *dst.CompositeLit:
		if e.Type == nil {
			return ""
		}
		// Typed literal identified by its type and first keyed field value, if any.
		first := ""
		if len(e.Elts) > 0 {
			first = exprSig(e.Elts[0])
		}
		return "lit:" + exprSig(e.Type) + "{" + first
	}
	return ""
}

// exprSig returns a canonical structural signature of an expression. Two expressions
// with the same signature are considered identical for merge purposes. Comments and
// formatting are ignored. Package-qualified identifiers resolved by the decorator
// (Ident with Path) are rendered with their full import path.
func exprSig(expr dst.Expr) string {
	var b strings.Builder
	writeExprSig(&b, expr)
	return b.String()
}

// writeExprSig writes the canonical signature of expr into b.
// nolint: funlen,gocyclo //Why: a flat type switch over expression kinds is clearest.
func writeExprSig(b *strings.Builder, expr dst.Expr) {
	writeList := func(list []dst.Expr) {
		for i, x := range list {
			if i > 0 {
				b.WriteByte(',')
			}
			writeExprSig(b, x)
		}
	}
	switch e := expr.(type) {
	case nil:
	case *dst.Ident:
		if e.Path != "" {
			b.WriteString(e.Path)
			b.WriteByte('.')
		}
		b.WriteString(e.Name)
	case *dst.BasicLit:
		b.WriteString(e.Value)
	case *dst.SelectorExpr:
		writeExprSig(b, e.X)
		b.WriteByte('.')
		b.WriteString(e.Sel.Name)
	case *dst.CallExpr:
		writeExprSig(b, e.Fun)
		b.WriteByte('(')
		writeList(e.Args)
		if e.Ellipsis {
			b.WriteString("...")
		}
		b.WriteByte(')')
	case *dst.CompositeLit:
		writeExprSig(b, e.Type)
		b.WriteByte('{')
		writeList(e.Elts)
		b.WriteByte('}')
	case *dst.KeyValueExpr:
		writeExprSig(b, e.Key)
		b.WriteByte(':')
		writeExprSig(b, e.Value)
	case *dst.UnaryExpr:
		b.WriteString(e.Op.String())
		writeExprSig(b, e.X)
	case *dst.BinaryExpr:
		writeExprSig(b, e.X)
		b.WriteString(e.Op.String())
		writeExprSig(b, e.Y)
	case *dst.StarExpr:
		b.WriteByte('*')
		writeExprSig(b, e.X)
	case *dst.ParenExpr:
		b.WriteByte('(')
		writeExprSig(b, e.X)
		b.WriteByte(')')
	case *dst.IndexExpr:
		writeExprSig(b, e.X)
		b.WriteByte('[')
		writeExprSig(b, e.Index)
		b.WriteByte(']')
	case *dst.IndexListExpr:
		writeExprSig(b, e.X)
		b.WriteByte('[')
		writeList(e.Indices)
		b.WriteByte(']')
	case *dst.SliceExpr:
		writeExprSig(b, e.X)
		b.WriteByte('[')
		writeExprSig(b, e.Low)
		b.WriteByte(':')
		writeExprSig(b, e.High)
		if e.Slice3 {
			b.WriteByte(':')
			writeExprSig(b, e.Max)
		}
		b.WriteByte(']')
	case *dst.TypeAssertExpr:
		writeExprSig(b, e.X)
		b.WriteString(".(")
		writeExprSig(b, e.Type)
		b.WriteByte(')')
	case *dst.ArrayType:
		b.WriteByte('[')
		writeExprSig(b, e.Len)
		b.WriteByte(']')
		writeExprSig(b, e.Elt)
	case *dst.MapType:
		b.WriteString("map[")
		writeExprSig(b, e.Key)
		b.WriteByte(']')
		writeExprSig(b, e.Value)
	case *dst.ChanType:
		fmt.Fprintf(b, "chan%d ", e.Dir)
		writeExprSig(b, e.Value)
	case *dst.Ellipsis:
		b.WriteString("...")
		writeExprSig(b, e.Elt)
	case *dst.FuncType:
		b.WriteString("func(")
		writeFieldsSig(b, e.Params)
		b.WriteString(")(")
		writeFieldsSig(b, e.Results)
		b.WriteByte(')')
	case *dst.FuncLit:
		// Function bodies are not compared; literals are identified by signature
		// and statement count.
		writeExprSig(b, e.Type)
		if e.Body != nil {
			fmt.Fprintf(b, "{%d}", len(e.Body.List))
		}
	default:
		fmt.Fprintf(b, "%T", e)
	}
}

// writeFieldsSig writes the types of a field list (names are ignored).
func writeFieldsSig(b *strings.Builder, fields *dst.FieldList) {
	if fields == nil {
		return
	}
	for i, f := range fields.List {
		if i > 0 {
			b.WriteByte(',')
		}
		n := len(f.Names)
		if n == 0 {
			n = 1
		}
		for j := 0; j < n; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			writeExprSig(b, f.Type)
		}
	}
}
