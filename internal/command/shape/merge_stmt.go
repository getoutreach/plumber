// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements statement-by-statement body merging for inplace function merge.
// Template statements must appear as an ordered subsequence in the existing body. Each matched
// statement is deep-merged to ensure call arguments and composite literal fields are present.

package shape

import (
	"fmt"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/internal/astx"
)

// mergeBody merges the generated (template) body into the existing function body.
// If the existing body is empty, all template statements are inserted.
// If the existing body is non-empty, template statements must appear as an ordered
// subsequence — if any template statement cannot be found, an error is returned.
func mergeBody(existing, generated *dst.BlockStmt, importMap map[string]string) error {
	if existing == nil {
		return fmt.Errorf("existing function body is nil")
	}

	// Empty body: insert all template statements
	if len(existing.List) == 0 {
		for _, stmt := range generated.List {
			s := dst.Clone(stmt).(dst.Stmt)
			annotateStmt(s, importMap)
			existing.List = append(existing.List, s)
		}
		return nil
	}

	// Non-empty body: template statements must be a subsequence
	existingIdx := 0
	for _, templateStmt := range generated.List {
		found := false
		for existingIdx < len(existing.List) {
			if statementsMatch(existing.List[existingIdx], templateStmt) {
				// Found — deep merge the matched statement
				deepMergeStmt(existing.List[existingIdx], templateStmt, importMap)
				existingIdx++
				found = true
				break
			}
			existingIdx++
		}
		if !found {
			return fmt.Errorf(
				"template statement not found as subsequence in existing body: %s",
				stmtKey(templateStmt),
			)
		}
	}

	return nil
}

// statementsMatch performs a shallow key-based match between two statements.
// It matches by the structural "key" of the statement (assignment LHS, call target,
// return keyword, etc.), not by full structural equality.
func statementsMatch(existing, template dst.Stmt) bool {
	switch t := template.(type) {
	case *dst.AssignStmt:
		e, ok := existing.(*dst.AssignStmt)
		if !ok {
			return false
		}
		// Match by LHS expressions
		return lhsMatch(e.Lhs, t.Lhs)

	case *dst.ExprStmt:
		e, ok := existing.(*dst.ExprStmt)
		if !ok {
			return false
		}
		// Match by call target
		return callTargetMatch(e.X, t.X)

	case *dst.ReturnStmt:
		_, ok := existing.(*dst.ReturnStmt)
		return ok

	case *dst.DeclStmt:
		e, ok := existing.(*dst.DeclStmt)
		if !ok {
			return false
		}
		return declStmtMatch(e, t)

	case *dst.SwitchStmt:
		e, ok := existing.(*dst.SwitchStmt)
		if !ok {
			return false
		}
		// Match by switch tag expression
		return exprKey(e.Tag) == exprKey(t.Tag)

	case *dst.IfStmt:
		_, ok := existing.(*dst.IfStmt)
		return ok

	case *dst.ForStmt:
		_, ok := existing.(*dst.ForStmt)
		return ok

	case *dst.RangeStmt:
		_, ok := existing.(*dst.RangeStmt)
		return ok

	default:
		// For other statement types, match by same type
		return fmt.Sprintf("%T", existing) == fmt.Sprintf("%T", template)
	}
}

// lhsMatch checks if the LHS expressions of two assignments match.
func lhsMatch(existing, template []dst.Expr) bool {
	if len(existing) != len(template) {
		return false
	}
	for i := range template {
		if exprKey(existing[i]) != exprKey(template[i]) {
			return false
		}
	}
	return true
}

// callTargetMatch checks if two call expressions target the same function.
func callTargetMatch(existing, template dst.Expr) bool {
	existingCall, ok1 := existing.(*dst.CallExpr)
	templateCall, ok2 := template.(*dst.CallExpr)
	if !ok1 || !ok2 {
		return false
	}
	return exprKey(existingCall.Fun) == exprKey(templateCall.Fun)
}

// declStmtMatch matches declaration statements by variable name.
func declStmtMatch(existing, template *dst.DeclStmt) bool {
	eDecl, ok1 := existing.Decl.(*dst.GenDecl)
	tDecl, ok2 := template.Decl.(*dst.GenDecl)
	if !ok1 || !ok2 || eDecl.Tok != tDecl.Tok {
		return false
	}
	if len(eDecl.Specs) == 0 || len(tDecl.Specs) == 0 {
		return false
	}
	eSpec, ok1 := eDecl.Specs[0].(*dst.ValueSpec)
	tSpec, ok2 := tDecl.Specs[0].(*dst.ValueSpec)
	if !ok1 || !ok2 {
		return false
	}
	if len(eSpec.Names) == 0 || len(tSpec.Names) == 0 {
		return false
	}
	return eSpec.Names[0].Name == tSpec.Names[0].Name
}

// deepMergeStmt recursively merges the contents of a matched statement.
// It augments call arguments and composite literal fields.
func deepMergeStmt(existing, template dst.Stmt, importMap map[string]string) {
	switch t := template.(type) {
	case *dst.AssignStmt:
		e, ok := existing.(*dst.AssignStmt)
		if !ok {
			return
		}
		// Merge RHS expressions pairwise
		for i := range t.Rhs {
			if i < len(e.Rhs) {
				deepMergeExpr(e.Rhs[i], t.Rhs[i], importMap)
			}
		}

	case *dst.ReturnStmt:
		e, ok := existing.(*dst.ReturnStmt)
		if !ok {
			return
		}
		// Merge return values pairwise
		for i := range t.Results {
			if i < len(e.Results) {
				deepMergeExpr(e.Results[i], t.Results[i], importMap)
			}
		}

	case *dst.ExprStmt:
		e, ok := existing.(*dst.ExprStmt)
		if !ok {
			return
		}
		deepMergeExpr(e.X, t.X, importMap)

	case *dst.SwitchStmt:
		e, ok := existing.(*dst.SwitchStmt)
		if !ok {
			return
		}
		mergeSwitchCases(e, t, importMap)
	}
}

// deepMergeExpr recursively merges expressions, augmenting call args and composite lit fields.
func deepMergeExpr(existing, template dst.Expr, importMap map[string]string) {
	switch t := template.(type) {
	case *dst.CallExpr:
		e, ok := existing.(*dst.CallExpr)
		if !ok {
			return
		}
		mergeCallArgs(e, t, importMap)

	case *dst.UnaryExpr:
		e, ok := existing.(*dst.UnaryExpr)
		if !ok {
			return
		}
		deepMergeExpr(e.X, t.X, importMap)

	case *dst.CompositeLit:
		e, ok := existing.(*dst.CompositeLit)
		if !ok {
			return
		}
		mergeCompositeLit(e, t, importMap)
	}
}

// mergeCallArgs ensures the existing call has at least all the arguments from the template.
// Arguments are matched structurally by their expression key.
func mergeCallArgs(existing, template *dst.CallExpr, importMap map[string]string) {
	// First, recursively merge args that already match
	for i, tArg := range template.Args {
		if i < len(existing.Args) {
			deepMergeExpr(existing.Args[i], tArg, importMap)
		}
	}

	// Build set of existing argument keys
	existingKeys := make(map[string]bool)
	for _, arg := range existing.Args {
		existingKeys[exprKey(arg)] = true
	}

	// Append missing arguments from template
	for _, tArg := range template.Args {
		key := exprKey(tArg)
		if key == "" {
			continue
		}
		if existingKeys[key] {
			continue
		}
		newArg := dst.Clone(tArg).(dst.Expr)
		annotateExpr(newArg, importMap)
		newArg.Decorations().Before = dst.NewLine
		newArg.Decorations().After = dst.NewLine
		existing.Args = append(existing.Args, newArg)
		existingKeys[key] = true
	}

	// Also recursively merge the function expression itself (for nested calls)
	deepMergeExpr(existing.Fun, template.Fun, importMap)
}

// mergeCompositeLit ensures the existing composite literal has at least all the
// key-value entries from the template. Entries are matched by key name.
func mergeCompositeLit(existing, template *dst.CompositeLit, importMap map[string]string) {
	// Build set of existing keys
	existingKeys := make(map[string]bool)
	for _, elt := range existing.Elts {
		if kv, ok := elt.(*dst.KeyValueExpr); ok {
			existingKeys[exprKey(kv.Key)] = true
		}
	}

	// For existing key-value pairs that match template entries, deep merge values
	for _, tElt := range template.Elts {
		tKV, ok := tElt.(*dst.KeyValueExpr)
		if !ok {
			continue
		}
		tKey := exprKey(tKV.Key)
		for _, eElt := range existing.Elts {
			eKV, ok := eElt.(*dst.KeyValueExpr)
			if !ok {
				continue
			}
			if exprKey(eKV.Key) == tKey {
				deepMergeExpr(eKV.Value, tKV.Value, importMap)
				break
			}
		}
	}

	// Append missing entries
	for _, tElt := range template.Elts {
		tKV, ok := tElt.(*dst.KeyValueExpr)
		if !ok {
			continue
		}
		tKey := exprKey(tKV.Key)
		if existingKeys[tKey] {
			continue
		}
		newElt := dst.Clone(tElt).(dst.Expr)
		annotateExpr(newElt, importMap)
		newElt.Decorations().Before = dst.NewLine
		newElt.Decorations().After = dst.NewLine
		existing.Elts = append(existing.Elts, newElt)
		existingKeys[tKey] = true
	}
}

// mergeSwitchCases ensures the existing switch statement has all case clauses from the template.
// Cases are matched by their case expression values. Missing cases are inserted after the last
// matched preceding case. The default clause is matched by having an empty case list.
func mergeSwitchCases(existing, template *dst.SwitchStmt, importMap map[string]string) {
	if existing.Body == nil || template.Body == nil {
		return
	}

	// Build index of existing case keys → position
	existingCaseIdx := make(map[string]int)
	for i, stmt := range existing.Body.List {
		cc, ok := stmt.(*dst.CaseClause)
		if !ok {
			continue
		}
		existingCaseIdx[caseClauseKey(cc)] = i
	}

	// Walk template cases. For each one, if it exists deep-merge the body,
	// otherwise insert it after the last matched case.
	lastInsertPos := 0
	for _, stmt := range template.Body.List {
		tCC, ok := stmt.(*dst.CaseClause)
		if !ok {
			continue
		}
		tKey := caseClauseKey(tCC)
		if idx, found := existingCaseIdx[tKey]; found {
			// Deep merge body statements of matched case
			eCC := existing.Body.List[idx].(*dst.CaseClause)
			mergeCaseBody(eCC, tCC, importMap)
			lastInsertPos = idx + 1
		} else {
			// Insert missing case clause at lastInsertPos
			newCC := dst.Clone(tCC).(*dst.CaseClause)
			annotateCaseClause(newCC, importMap)

			// Insert into existing body list
			list := existing.Body.List
			updated := make([]dst.Stmt, 0, len(list)+1)
			updated = append(updated, list[:lastInsertPos]...)
			updated = append(updated, newCC)
			updated = append(updated, list[lastInsertPos:]...)
			existing.Body.List = updated

			// Update index positions for items shifted right
			for k, v := range existingCaseIdx {
				if v >= lastInsertPos {
					existingCaseIdx[k] = v + 1
				}
			}
			existingCaseIdx[tKey] = lastInsertPos
			lastInsertPos++
		}
	}
}

// caseClauseKey returns a match key for a case clause.
// Default clause (empty List) returns "default". Regular cases return their expression keys joined.
func caseClauseKey(cc *dst.CaseClause) string {
	if len(cc.List) == 0 {
		return "default"
	}
	key := ""
	for i, expr := range cc.List {
		if i > 0 {
			key += ","
		}
		key += exprKey(expr)
	}
	return key
}

// mergeCaseBody deep-merges the body of a matched case clause.
// Template body statements are merged into the existing case body using the same
// subsequence logic as function bodies.
func mergeCaseBody(existing, template *dst.CaseClause, importMap map[string]string) {
	if len(template.Body) == 0 {
		return
	}
	// For each template body statement, try to find and deep-merge in existing
	for _, tStmt := range template.Body {
		for _, eStmt := range existing.Body {
			if statementsMatch(eStmt, tStmt) {
				deepMergeStmt(eStmt, tStmt, importMap)
				break
			}
		}
	}
}

// annotateCaseClause annotates expressions in a case clause for proper import resolution.
func annotateCaseClause(cc *dst.CaseClause, importMap map[string]string) {
	for i, expr := range cc.List {
		cc.List[i] = astx.RewriteExpr(expr, importMap)
	}
	for _, stmt := range cc.Body {
		annotateStmt(stmt, importMap)
	}
}

// exprKey returns a string key for an expression used for structural matching.
// It captures the identity of the expression (variable name, selector path, etc.)
// without comparing full structural equality.
func exprKey(expr dst.Expr) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *dst.Ident:
		return e.Name
	case *dst.SelectorExpr:
		return exprKey(e.X) + "." + e.Sel.Name
	case *dst.StarExpr:
		return "*" + exprKey(e.X)
	case *dst.UnaryExpr:
		return e.Op.String() + exprKey(e.X)
	case *dst.CallExpr:
		return exprKey(e.Fun) + "()"
	case *dst.IndexExpr:
		return exprKey(e.X) + "[" + exprKey(e.Index) + "]"
	case *dst.CompositeLit:
		if e.Type != nil {
			return exprKey(e.Type) + "{}"
		}
		return "{}"
	case *dst.BasicLit:
		return e.Value
	case *dst.FuncLit:
		return "func(){}"
	default:
		return fmt.Sprintf("%T", e)
	}
}

// stmtKey returns a human-readable key for a statement for error messages.
func stmtKey(stmt dst.Stmt) string {
	switch s := stmt.(type) {
	case *dst.AssignStmt:
		if len(s.Lhs) > 0 {
			return fmt.Sprintf("assign(%s)", exprKey(s.Lhs[0]))
		}
		return "assign"
	case *dst.ExprStmt:
		return fmt.Sprintf("expr(%s)", exprKey(s.X))
	case *dst.ReturnStmt:
		return "return"
	case *dst.DeclStmt:
		return "decl"
	case *dst.SwitchStmt:
		return fmt.Sprintf("switch(%s)", exprKey(s.Tag))
	default:
		return fmt.Sprintf("%T", s)
	}
}

// annotateStmt rewrites all type/package-qualified expressions in a statement.
func annotateStmt(stmt dst.Stmt, importMap map[string]string) {
	switch s := stmt.(type) {
	case *dst.AssignStmt:
		for i, expr := range s.Rhs {
			s.Rhs[i] = astx.RewriteExpr(expr, importMap)
		}
	case *dst.ReturnStmt:
		for i, expr := range s.Results {
			s.Results[i] = astx.RewriteExpr(expr, importMap)
		}
	case *dst.ExprStmt:
		s.X = astx.RewriteExpr(s.X, importMap)
	case *dst.DeclStmt:
		if gd, ok := s.Decl.(*dst.GenDecl); ok {
			for _, spec := range gd.Specs {
				if vs, ok := spec.(*dst.ValueSpec); ok {
					vs.Type = astx.RewriteExpr(vs.Type, importMap)
					for i, v := range vs.Values {
						vs.Values[i] = astx.RewriteExpr(v, importMap)
					}
				}
			}
		}
	case *dst.SwitchStmt:
		s.Tag = astx.RewriteExpr(s.Tag, importMap)
		if s.Body != nil {
			for _, stmt := range s.Body.List {
				if cc, ok := stmt.(*dst.CaseClause); ok {
					annotateCaseClause(cc, importMap)
				}
			}
		}
	}
}

// annotateExpr rewrites package-qualified expressions using the import map.
func annotateExpr(expr dst.Expr, importMap map[string]string) {
	astx.RewriteExpr(expr, importMap)
}

// annotateBlockStmt annotates all statements in a block for proper import resolution.
func annotateBlockStmt(block *dst.BlockStmt, importMap map[string]string) {
	for _, stmt := range block.List {
		annotateStmt(stmt, importMap)
	}
}
