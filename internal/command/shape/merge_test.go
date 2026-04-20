// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for the extended inplace merge logic covering functions, methods,
// variables, statement-by-statement body merging, and call argument augmentation.

package shape

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/astx/inspect"
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

// mergeTestFixtureDir creates a temporary fixture directory inside the current
// module tree so that go/packages can load it. Returns the directory path.
func mergeTestFixtureDir(t *testing.T, name string) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	err = os.MkdirAll(dir, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll("testdata") })
	return dir
}

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

func TestHasGenerateOnce(t *testing.T) {
	tests := []struct {
		name   string
		src    string
		expect bool
	}{
		{
			name: "function with generate:once annotation",
			src: `package p

// generate:once
func Define() {}
`,
			expect: true,
		},
		{
			name: "function without annotation",
			src: `package p

// Define sets up the container
func Define() {}
`,
			expect: false,
		},
		{
			name: "function with mixed comments and generate:once",
			src: `package p

// Define sets up the container
//
// generate:once
func Define() {}
`,
			expect: true,
		},
		{
			name:   "empty decorations",
			src:    "package p\nfunc f() {}\n",
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := parseFile(t, tt.src)
			fd := f.Decls[0].(*dst.FuncDecl)
			got := hasGenerateOnce(fd.Decs.Start)
			if got != tt.expect {
				t.Errorf("hasGenerateOnce() = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestHasGenerateOnce_Struct(t *testing.T) {
	src := `package p

// generate:once
type Foo struct {
	A int
}
`
	f := parseFile(t, src)
	gd := f.Decls[0].(*dst.GenDecl)
	got := hasGenerateOnce(gd.Decs.Start)
	if !got {
		t.Error("expected hasGenerateOnce to return true for annotated struct")
	}
}

func TestHasGenerateOnce_StructWithout(t *testing.T) {
	src := `package p

// Foo is a thing
type Foo struct {
	A int
}
`
	f := parseFile(t, src)
	gd := f.Decls[0].(*dst.GenDecl)
	got := hasGenerateOnce(gd.Decs.Start)
	if got {
		t.Error("expected hasGenerateOnce to return false for non-annotated struct")
	}
}

// TestMergeGenerateOnce_FuncSkipsWhenExists is an acceptance test verifying that
// the Merge function skips a function declaration annotated with generate:once
// when the function already exists in the target package.
func TestMergeGenerateOnce_FuncSkipsWhenExists(t *testing.T) {
	dir := mergeTestFixtureDir(t, "generate_once_func_skip")

	// Write existing source with a Define method that has custom user content
	existingSrc := `package generate_once_func_skip

type Container struct {
	Name string
}

// Define sets up the container with custom logic
func (c *Container) Define() {
	c.Name = "custom"
}
`
	err := os.WriteFile(filepath.Join(dir, "container.go"), []byte(existingSrc), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	// Load the package using inspect
	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "container.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	if len(pkgs) == 0 {
		t.Fatal("no packages loaded")
	}
	pkg := pkgs[0]

	// Parse generated code WITH generate:once annotation
	generatedSrc := `package generate_once_func_skip

// generate:once
func (c *Container) Define() {
	c.Name = "generated"
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	// Merge should skip the function because it exists and has generate:once
	resultFile, err := Merge(pkg, generatedFile)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// resultFile should be nil because nothing was merged
	if resultFile != nil {
		t.Fatal("expected nil result (no merge performed), but got a file")
	}
}

// TestMergeGenerateOnce_FuncAddsWhenNotExists verifies that a function annotated
// with generate:once IS added when it doesn't exist yet.
func TestMergeGenerateOnce_FuncAddsWhenNotExists(t *testing.T) {
	dir := mergeTestFixtureDir(t, "generate_once_func_add")

	// Existing source — Container exists but has no Define method
	existingSrc := `package generate_once_func_add

type Container struct {
	Name string
}
`
	err := os.WriteFile(filepath.Join(dir, "container.go"), []byte(existingSrc), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "container.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	// Generated code with generate:once — function does NOT exist yet, so it should be added
	generatedSrc := `package generate_once_func_add

// generate:once
func (c *Container) Define() {
	c.Name = "generated"
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFile, err := Merge(pkg, generatedFile)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Function should have been added
	if resultFile == nil {
		t.Fatal("expected a file with the added function, got nil")
	}

	// Verify the function was added
	found := findFuncDecl(resultFile, "Define")
	if found == nil {
		t.Fatal("expected Define function to be added to the file")
	}
}

// TestMergeGenerateOnce_FuncMergesWithoutAnnotation verifies that without
// generate:once, a function IS merged even when it already exists (normal behavior).
func TestMergeGenerateOnce_FuncMergesWithoutAnnotation(t *testing.T) {
	dir := mergeTestFixtureDir(t, "generate_once_func_merge")

	existingSrc := `package generate_once_func_merge

type Container struct {
	Name string
}

func (c *Container) Define() {
	c.Name = "original"
}
`
	err := os.WriteFile(filepath.Join(dir, "container.go"), []byte(existingSrc), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "container.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	// Generated code WITHOUT generate:once — should merge normally
	generatedSrc := `package generate_once_func_merge

func (c *Container) Define() {
	c.Name = "original"
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFile, err := Merge(pkg, generatedFile)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Should have merged (returned a file)
	if resultFile == nil {
		t.Fatal("expected merge to proceed (no generate:once), but got nil")
	}
}

// TestMergeGenerateOnce_StructSkipsWhenExists verifies that a struct annotated
// with generate:once skips field merging when the struct already exists.
func TestMergeGenerateOnce_StructSkipsWhenExists(t *testing.T) {
	dir := mergeTestFixtureDir(t, "generate_once_struct_skip")

	// Existing source with Container that has only Name field
	existingSrc := `package generate_once_struct_skip

type Container struct {
	Name string
}
`
	err := os.WriteFile(filepath.Join(dir, "container.go"), []byte(existingSrc), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "container.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	// Generated code tries to add a new field, but has generate:once
	generatedSrc := `package generate_once_struct_skip

// generate:once
type Container struct {
	Name  string
	Extra int
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFile, err := Merge(pkg, generatedFile)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Should be nil — struct exists and generate:once is set, so merge is skipped
	if resultFile != nil {
		t.Fatal("expected nil result (struct merge skipped), but got a file")
	}
}
