// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for the extended inplace merge logic covering functions, methods,
// variables, statement-by-statement body merging, and call argument augmentation.

package shape

import (
	"testing"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
)

func TestStatementsMatch(t *testing.T) {
	tests := []struct {
		name     string
		existing string
		template string
		match    bool
	}{
		{
			name:     "assign match by LHS",
			existing: `a := foo()`,
			template: `a := bar()`,
			match:    true,
		},
		{
			name:     "assign no match different LHS",
			existing: `b := foo()`,
			template: `a := bar()`,
			match:    false,
		},
		{
			name:     "return matches return",
			existing: `return nil`,
			template: `return x`,
			match:    true,
		},
		{
			name:     "expr call match by target",
			existing: `foo.Bar(x, y)`,
			template: `foo.Bar(z)`,
			match:    true,
		},
		{
			name:     "expr call no match different target",
			existing: `foo.Bar(x)`,
			template: `foo.Baz(x)`,
			match:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existingStmt := parseStmt(t, tt.existing)
			templateStmt := parseStmt(t, tt.template)
			got := statementsMatch(existingStmt, templateStmt)
			if got != tt.match {
				t.Errorf("statementsMatch() = %v, want %v", got, tt.match)
			}
		})
	}
}

func TestMergeBody_EmptyExisting(t *testing.T) {
	existing := &dst.BlockStmt{List: []dst.Stmt{}}
	templateBody := parseBody(t, `
		a := new(Foo)
		return a
	`)

	importMap := map[string]string{}
	err := mergeBody(existing, templateBody, importMap)
	if err != nil {
		t.Fatalf("mergeBody() error = %v", err)
	}
	if len(existing.List) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(existing.List))
	}
}

func TestMergeBody_SubsequencePresent(t *testing.T) {
	existingBody := parseBody(t, `
		a := new(Foo)
		setupLogging()
		return a
	`)
	templateBody := parseBody(t, `
		a := new(Foo)
		return a
	`)

	importMap := map[string]string{}
	err := mergeBody(existingBody, templateBody, importMap)
	if err != nil {
		t.Fatalf("mergeBody() error = %v", err)
	}
	// Should remain unchanged (3 stmts: the two template + the extra)
	if len(existingBody.List) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(existingBody.List))
	}
}

func TestMergeBody_MissingStatement_Error(t *testing.T) {
	existingBody := parseBody(t, `
		a := new(Foo)
	`)
	templateBody := parseBody(t, `
		a := new(Foo)
		return a
	`)

	importMap := map[string]string{}
	err := mergeBody(existingBody, templateBody, importMap)
	if err == nil {
		t.Fatalf("expected error for missing template statement, got nil")
	}
}

func TestMergeCallArgs(t *testing.T) {
	existingSrc := `package p
func f() { foo(a, b) }
`
	templateSrc := `package p
func f() { foo(a, b, c) }
`
	existingFile := parseFile(t, existingSrc)
	templateFile := parseFile(t, templateSrc)

	existingCall := findFirstCall(existingFile)
	templateCall := findFirstCall(templateFile)

	if existingCall == nil || templateCall == nil {
		t.Fatal("could not find call expressions")
	}

	mergeCallArgs(existingCall, templateCall, map[string]string{})

	// Should now have 3 args
	if len(existingCall.Args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(existingCall.Args))
	}
}

func TestMergeCallArgs_ExtraExistingFine(t *testing.T) {
	existingSrc := `package p
func f() { foo(a, b, c, d) }
`
	templateSrc := `package p
func f() { foo(a, b, c) }
`
	existingFile := parseFile(t, existingSrc)
	templateFile := parseFile(t, templateSrc)

	existingCall := findFirstCall(existingFile)
	templateCall := findFirstCall(templateFile)

	mergeCallArgs(existingCall, templateCall, map[string]string{})

	// Should remain at 4 args (d is extra, that's fine)
	if len(existingCall.Args) != 4 {
		t.Fatalf("expected 4 args, got %d", len(existingCall.Args))
	}
}

func TestMergeCompositeLit(t *testing.T) {
	existingSrc := `package p
func f() { _ = &T{A: 1} }
`
	templateSrc := `package p
func f() { _ = &T{A: 1, B: 2} }
`
	existingFile := parseFile(t, existingSrc)
	templateFile := parseFile(t, templateSrc)

	existingLit := findFirstCompositeLit(existingFile)
	templateLit := findFirstCompositeLit(templateFile)

	if existingLit == nil || templateLit == nil {
		t.Fatal("could not find composite literals")
	}

	mergeCompositeLit(existingLit, templateLit, map[string]string{})

	if len(existingLit.Elts) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(existingLit.Elts))
	}
}

func TestMergeParams(t *testing.T) {
	existingSrc := `package p
func f(a int) {}
`
	templateSrc := `package p
func f(a int, b string) {}
`
	existingFile := parseFile(t, existingSrc)
	templateFile := parseFile(t, templateSrc)

	existingFunc := existingFile.Decls[0].(*dst.FuncDecl)
	templateFunc := templateFile.Decls[0].(*dst.FuncDecl)

	err := mergeParams(existingFunc, templateFunc, map[string]string{})
	if err != nil {
		t.Fatalf("mergeParams() error = %v", err)
	}

	if len(existingFunc.Type.Params.List) != 2 {
		t.Fatalf("expected 2 params, got %d", len(existingFunc.Type.Params.List))
	}
}

func TestExprKey(t *testing.T) {
	tests := []struct {
		src      string
		expected string
	}{
		{"a", "a"},
		{"a.B", "a.B"},
		{"a.B.C", "a.B.C"},
		{"foo()", "foo()"},
		{"a.Foo()", "a.Foo()"},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			expr := parseExpr(t, tt.src)
			got := exprKey(expr)
			if got != tt.expected {
				t.Errorf("exprKey(%q) = %q, want %q", tt.src, got, tt.expected)
			}
		})
	}
}

// --- helpers ---

func parseFile(t *testing.T, src string) *dst.File {
	t.Helper()
	f, err := decorator.Parse(src)
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}
	return f
}

func parseStmt(t *testing.T, stmtSrc string) dst.Stmt {
	t.Helper()
	src := "package p\nfunc f() {\n" + stmtSrc + "\n}\n"
	f := parseFile(t, src)
	fd := f.Decls[0].(*dst.FuncDecl)
	if len(fd.Body.List) == 0 {
		t.Fatalf("no statements parsed from: %s", stmtSrc)
	}
	return fd.Body.List[0]
}

func parseBody(t *testing.T, bodySrc string) *dst.BlockStmt {
	t.Helper()
	src := "package p\nfunc f() {\n" + bodySrc + "\n}\n"
	f := parseFile(t, src)
	fd := f.Decls[0].(*dst.FuncDecl)
	return fd.Body
}

func parseExpr(t *testing.T, exprSrc string) dst.Expr {
	t.Helper()
	src := "package p\nvar _ = " + exprSrc + "\n"
	f := parseFile(t, src)
	gd := f.Decls[0].(*dst.GenDecl)
	vs := gd.Specs[0].(*dst.ValueSpec)
	return vs.Values[0]
}

func findFirstCall(file *dst.File) *dst.CallExpr {
	var result *dst.CallExpr
	dst.Inspect(file, func(n dst.Node) bool {
		if result != nil {
			return false
		}
		if c, ok := n.(*dst.CallExpr); ok {
			result = c
			return false
		}
		return true
	})
	return result
}

func findFirstCompositeLit(file *dst.File) *dst.CompositeLit {
	var result *dst.CompositeLit
	dst.Inspect(file, func(n dst.Node) bool {
		if result != nil {
			return false
		}
		if c, ok := n.(*dst.CompositeLit); ok {
			result = c
			return false
		}
		return true
	})
	return result
}

func TestStatementsMatchSwitch(t *testing.T) {
	tests := []struct {
		name     string
		existing string
		template string
		match    bool
	}{
		{
			name:     "switch match by tag",
			existing: `switch x {}`,
			template: `switch x {}`,
			match:    true,
		},
		{
			name:     "switch no match different tag",
			existing: `switch x {}`,
			template: `switch y {}`,
			match:    false,
		},
		{
			name:     "switch not matching non-switch",
			existing: `x := 1`,
			template: `switch x {}`,
			match:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eStmt := parseStmt(t, tt.existing)
			tStmt := parseStmt(t, tt.template)
			got := statementsMatch(eStmt, tStmt)
			if got != tt.match {
				t.Errorf("statementsMatch() = %v, want %v", got, tt.match)
			}
		})
	}
}

func TestMergeSwitchCases(t *testing.T) {
	t.Run("adds missing case clause", func(t *testing.T) {
		existingSrc := `package p
func f() {
	switch x {
	case "a":
		foo()
	default:
		bar()
	}
}`
		templateSrc := `package p
func f() {
	switch x {
	case "a":
		foo()
	case "b":
		baz()
	default:
		bar()
	}
}`
		eFile, err := decorator.Parse(existingSrc)
		if err != nil {
			t.Fatal(err)
		}
		tFile, err := decorator.Parse(templateSrc)
		if err != nil {
			t.Fatal(err)
		}

		eSwitch := findFirstSwitch(eFile)
		tSwitch := findFirstSwitch(tFile)
		if eSwitch == nil || tSwitch == nil {
			t.Fatal("could not find switch statements")
		}

		mergeSwitchCases(eSwitch, tSwitch, nil)

		// Should now have 3 case clauses: "a", "b", default
		if len(eSwitch.Body.List) != 3 {
			t.Fatalf("expected 3 case clauses, got %d", len(eSwitch.Body.List))
		}

		// Verify order: "a", "b", default
		keys := make([]string, 0, 3)
		for _, stmt := range eSwitch.Body.List {
			cc := stmt.(*dst.CaseClause)
			keys = append(keys, caseClauseKey(cc))
		}
		expected := []string{`"a"`, `"b"`, "default"}
		for i, k := range keys {
			if k != expected[i] {
				t.Errorf("case %d: got key %q, want %q", i, k, expected[i])
			}
		}
	})

	t.Run("preserves extra existing cases", func(t *testing.T) {
		existingSrc := `package p
func f() {
	switch x {
	case "a":
		foo()
	case "c":
		extra()
	default:
		bar()
	}
}`
		templateSrc := `package p
func f() {
	switch x {
	case "a":
		foo()
	case "b":
		baz()
	default:
		bar()
	}
}`
		eFile, err := decorator.Parse(existingSrc)
		if err != nil {
			t.Fatal(err)
		}
		tFile, err := decorator.Parse(templateSrc)
		if err != nil {
			t.Fatal(err)
		}

		eSwitch := findFirstSwitch(eFile)
		tSwitch := findFirstSwitch(tFile)

		mergeSwitchCases(eSwitch, tSwitch, nil)

		// Should have 4 case clauses: "a", "b", "c", default
		if len(eSwitch.Body.List) != 4 {
			t.Fatalf("expected 4 case clauses, got %d", len(eSwitch.Body.List))
		}

		keys := make([]string, 0, 4)
		for _, stmt := range eSwitch.Body.List {
			cc := stmt.(*dst.CaseClause)
			keys = append(keys, caseClauseKey(cc))
		}
		expected := []string{`"a"`, `"b"`, `"c"`, "default"}
		for i, k := range keys {
			if k != expected[i] {
				t.Errorf("case %d: got key %q, want %q", i, k, expected[i])
			}
		}
	})

	t.Run("deep merges existing case body", func(t *testing.T) {
		existingSrc := `package p
func f() {
	switch x {
	case "a":
		foo(1)
	}
}`
		templateSrc := `package p
func f() {
	switch x {
	case "a":
		foo(1, 2)
	}
}`
		eFile, err := decorator.Parse(existingSrc)
		if err != nil {
			t.Fatal(err)
		}
		tFile, err := decorator.Parse(templateSrc)
		if err != nil {
			t.Fatal(err)
		}

		eSwitch := findFirstSwitch(eFile)
		tSwitch := findFirstSwitch(tFile)

		mergeSwitchCases(eSwitch, tSwitch, nil)

		// The call in case "a" should now have 2 args
		cc := eSwitch.Body.List[0].(*dst.CaseClause)
		exprStmt := cc.Body[0].(*dst.ExprStmt)
		call := exprStmt.X.(*dst.CallExpr)
		if len(call.Args) != 2 {
			t.Errorf("expected 2 args after deep merge, got %d", len(call.Args))
		}
	})
}

func findFirstSwitch(file *dst.File) *dst.SwitchStmt {
	var result *dst.SwitchStmt
	dst.Inspect(file, func(n dst.Node) bool {
		if result != nil {
			return false
		}
		if s, ok := n.(*dst.SwitchStmt); ok {
			result = s
			return false
		}
		return true
	})
	return result
}
