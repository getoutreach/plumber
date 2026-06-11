// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for the extended inplace merge logic covering functions, methods,
// variables, statement-by-statement body merging, and call argument augmentation.

package shape

import (
	"os"
	"path/filepath"
	"strings"
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
	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// resultFiles should be empty because nothing was merged
	if len(resultFiles) != 0 {
		t.Fatalf("expected no merged files (no merge performed), but got %d", len(resultFiles))
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

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Function should have been added — exactly one file should be touched.
	if len(resultFiles) == 0 {
		t.Fatal("expected a file with the added function, got none")
	}

	// Verify the function was added to (one of) the returned files.
	var found *dst.FuncDecl
	for _, f := range resultFiles {
		if fd := findFuncDecl(f, "Define"); fd != nil {
			found = fd
			break
		}
	}
	if found == nil {
		t.Fatal("expected Define function to be added to one of the returned files")
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

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Should have merged (returned at least one file).
	if len(resultFiles) == 0 {
		t.Fatal("expected merge to proceed (no generate:once), but got no files")
	}
}

// --- Interface merging tests ---

// TestMergeInterface_AddsMethods verifies that new methods from a generated interface
// are appended to an existing interface.
func TestMergeInterface_AddsMethods(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_iface_add_methods")

	existingSrc := `package merge_iface_add_methods

type Reader interface {
	Read(p []byte) (n int, err error)
}
`
	err := os.WriteFile(filepath.Join(dir, "iface.go"), []byte(existingSrc), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "iface.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	generatedSrc := `package merge_iface_add_methods

type Reader interface {
	Read(p []byte) (n int, err error)
	Close() error
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a merged file, got none")
	}

	// Find the interface in the result and verify it has 2 methods
	ifaceType := findInterfaceType(t, resultFiles[0], "Reader")
	if ifaceType.Methods == nil || len(ifaceType.Methods.List) != 2 {
		t.Fatalf("expected 2 methods, got %d", countMethods(ifaceType))
	}
}

// TestMergeInterface_DeduplicatesMethods verifies that methods already present
// in the existing interface are not duplicated.
func TestMergeInterface_DeduplicatesMethods(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_iface_dedup")

	existingSrc := `package merge_iface_dedup

type Service interface {
	Start() error
	Stop() error
}
`
	err := os.WriteFile(filepath.Join(dir, "iface.go"), []byte(existingSrc), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "iface.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	// Generated has Start (existing) and Health (new)
	generatedSrc := `package merge_iface_dedup

type Service interface {
	Start() error
	Health() bool
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a merged file, got none")
	}

	// Should have 3 methods: Start, Stop, Health
	ifaceType := findInterfaceType(t, resultFiles[0], "Service")
	if countMethods(ifaceType) != 3 {
		t.Fatalf("expected 3 methods, got %d", countMethods(ifaceType))
	}
}

// TestMergeInterface_EmbeddedInterfaces verifies that embedded interfaces are
// merged and deduplicated by type expression.
func TestMergeInterface_EmbeddedInterfaces(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_iface_embed")

	existingSrc := `package merge_iface_embed

import "io"

type ReadCloser interface {
	io.Reader
}
`
	err := os.WriteFile(filepath.Join(dir, "iface.go"), []byte(existingSrc), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "iface.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	// Generated has io.Reader (existing) and io.Closer (new)
	generatedSrc := `package merge_iface_embed

import "io"

type ReadCloser interface {
	io.Reader
	io.Closer
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a merged file, got none")
	}

	// Should have 2 entries: io.Reader, io.Closer
	ifaceType := findInterfaceType(t, resultFiles[0], "ReadCloser")
	if countMethods(ifaceType) != 2 {
		t.Fatalf("expected 2 entries (embeds), got %d", countMethods(ifaceType))
	}
}

// TestMergeInterface_MixedMethodsAndEmbeds verifies merging when an interface
// has both methods and embedded interfaces.
func TestMergeInterface_MixedMethodsAndEmbeds(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_iface_mixed")

	existingSrc := `package merge_iface_mixed

import "io"

type Handler interface {
	io.Closer
	Handle(req []byte) error
}
`
	err := os.WriteFile(filepath.Join(dir, "iface.go"), []byte(existingSrc), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "iface.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	// Generated adds io.Reader (new embed), Handle (dup method), Reset (new method)
	generatedSrc := `package merge_iface_mixed

import "io"

type Handler interface {
	io.Closer
	io.Reader
	Handle(req []byte) error
	Reset()
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a merged file, got none")
	}

	// Should have 4 entries: io.Closer, Handle, io.Reader, Reset
	ifaceType := findInterfaceType(t, resultFiles[0], "Handler")
	if countMethods(ifaceType) != 4 {
		t.Fatalf("expected 4 entries, got %d", countMethods(ifaceType))
	}
}

// TestMergeInterface_NewInterfaceAdded verifies that a brand new interface
// is added to the package when it doesn't exist.
func TestMergeInterface_NewInterfaceAdded(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_iface_new")

	existingSrc := `package merge_iface_new

type Foo struct{}
`
	err := os.WriteFile(filepath.Join(dir, "types.go"), []byte(existingSrc), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "types.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	generatedSrc := `package merge_iface_new

type Service interface {
	Start() error
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "types.go")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a result file, got none")
	}

	// Verify the interface was added
	ifaceType := findInterfaceType(t, resultFiles[0], "Service")
	if countMethods(ifaceType) != 1 {
		t.Fatalf("expected 1 method, got %d", countMethods(ifaceType))
	}
}

// TestMergeInterface_GenerateOnceSkips verifies that generate:once skips
// interface merging when the interface already exists.
func TestMergeInterface_GenerateOnceSkips(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_iface_gen_once")

	existingSrc := `package merge_iface_gen_once

type Service interface {
	Start() error
}
`
	err := os.WriteFile(filepath.Join(dir, "iface.go"), []byte(existingSrc), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "iface.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	generatedSrc := `package merge_iface_gen_once

// generate:once
type Service interface {
	Start() error
	Stop() error
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Should be empty — interface exists and generate:once is set
	if len(resultFiles) != 0 {
		t.Fatalf("expected no merged files (generate:once skipped), but got %d", len(resultFiles))
	}
}

// --- Interface test helpers ---

func findInterfaceType(t *testing.T, file *dst.File, name string) *dst.InterfaceType {
	t.Helper()
	// First try scope lookup (works for types that existed before merge).
	if obj := file.Scope.Lookup(name); obj != nil {
		if ts, ok := obj.Decl.(*dst.TypeSpec); ok {
			if iface, ok := ts.Type.(*dst.InterfaceType); ok {
				return iface
			}
		}
	}
	// Fallback: iterate decls (works for newly-added types not yet in scope).
	for _, decl := range file.Decls {
		gd, ok := decl.(*dst.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*dst.TypeSpec)
			if !ok || ts.Name.Name != name {
				continue
			}
			if iface, ok := ts.Type.(*dst.InterfaceType); ok {
				return iface
			}
		}
	}
	t.Fatalf("interface type %q not found in file", name)
	return nil
}

func countMethods(iface *dst.InterfaceType) int {
	if iface.Methods == nil {
		return 0
	}
	return len(iface.Methods.List)
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

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Should be empty — struct exists and generate:once is set, so merge is skipped.
	if len(resultFiles) != 0 {
		t.Fatalf("expected no merged files (struct merge skipped), but got %d", len(resultFiles))
	}
}

// --- Doc comment merging tests ---

func TestHasDocComment(t *testing.T) {
	tests := []struct {
		name string
		decs dst.Decorations
		want bool
	}{
		{name: "nil", decs: nil, want: false},
		{name: "empty", decs: dst.Decorations{}, want: false},
		{name: "only newline", decs: dst.Decorations{"\n"}, want: false},
		{name: "line comment", decs: dst.Decorations{"// hello"}, want: true},
		{name: "block comment", decs: dst.Decorations{"/* hello */"}, want: true},
		{name: "mixed", decs: dst.Decorations{"\n", "// hello", "\n"}, want: true},
		{name: "leading spaces", decs: dst.Decorations{"   // hello"}, want: true},
		{name: "non comment string", decs: dst.Decorations{"hello"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasDocComment(tt.decs); got != tt.want {
				t.Errorf("hasDocComment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeDocComment(t *testing.T) {
	// extractItems pulls only the comment items from a Decorations slice so
	// tests can assert ordering and content without depending on the "\n"
	// separator pattern produced by rebuildDecorations.
	extractItems := func(decs dst.Decorations) []string {
		out := []string{}
		for _, s := range decs {
			t := strings.TrimSpace(s)
			if strings.HasPrefix(t, "//") || strings.HasPrefix(t, "/*") {
				out = append(out, s)
			}
		}
		return out
	}

	t.Run("empty existing adopts generated", func(t *testing.T) {
		existing := dst.Decorations{}
		generated := dst.Decorations{"// generated doc"}
		changed := mergeDocComment(&existing, generated)
		if !changed {
			t.Fatal("expected mergeDocComment to report change")
		}
		got := extractItems(existing)
		if len(got) != 1 || got[0] != "// generated doc" {
			t.Fatalf("expected existing to be adopted from generated, got %v", existing)
		}
	})

	t.Run("duplicate generated line is not re-added", func(t *testing.T) {
		existing := dst.Decorations{"// manual doc", "\n"}
		generated := dst.Decorations{"// manual doc"}
		changed := mergeDocComment(&existing, generated)
		if changed {
			t.Fatal("expected mergeDocComment to be a no-op when line already present")
		}
		got := extractItems(existing)
		if len(got) != 1 || got[0] != "// manual doc" {
			t.Fatalf("expected existing unchanged, got %v", existing)
		}
	})

	t.Run("inserts after common anchor", func(t *testing.T) {
		// existing = [c1, c3], generated = [c1, c2] → [c1, c2, c3]
		existing := dst.Decorations{"// c1", "\n", "// c3", "\n"}
		generated := dst.Decorations{"// c1", "\n", "// c2"}
		changed := mergeDocComment(&existing, generated)
		if !changed {
			t.Fatal("expected mergeDocComment to report change")
		}
		got := extractItems(existing)
		want := []string{"// c1", "// c2", "// c3"}
		if !equalStringSlices(got, want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("appends when no common anchor", func(t *testing.T) {
		// existing = [m1], generated = [g1] → [m1, g1]
		existing := dst.Decorations{"// m1", "\n"}
		generated := dst.Decorations{"// g1"}
		changed := mergeDocComment(&existing, generated)
		if !changed {
			t.Fatal("expected mergeDocComment to report change")
		}
		got := extractItems(existing)
		want := []string{"// m1", "// g1"}
		if !equalStringSlices(got, want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("progressive anchors", func(t *testing.T) {
		// existing = [c1, manual, c3], generated = [c1, c2, c3]
		// → [c1, c2, manual, c3]: c2 inserted after c1; c3 anchors to existing c3.
		existing := dst.Decorations{"// c1", "\n", "// manual", "\n", "// c3", "\n"}
		generated := dst.Decorations{"// c1", "\n", "// c2", "\n", "// c3"}
		changed := mergeDocComment(&existing, generated)
		if !changed {
			t.Fatal("expected mergeDocComment to report change")
		}
		got := extractItems(existing)
		want := []string{"// c1", "// c2", "// manual", "// c3"}
		if !equalStringSlices(got, want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("multiple new lines after last anchor", func(t *testing.T) {
		// existing = [c1], generated = [c1, c2, c3] → [c1, c2, c3]
		existing := dst.Decorations{"// c1", "\n"}
		generated := dst.Decorations{"// c1", "\n", "// c2", "\n", "// c3"}
		changed := mergeDocComment(&existing, generated)
		if !changed {
			t.Fatal("expected mergeDocComment to report change")
		}
		got := extractItems(existing)
		want := []string{"// c1", "// c2", "// c3"}
		if !equalStringSlices(got, want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("prefix-trim equality matches whitespace and double-slash variants", func(t *testing.T) {
		// Different leading whitespace and slash spacing should compare equal.
		existing := dst.Decorations{" //   hello", "\n"}
		generated := dst.Decorations{"//hello"}
		changed := mergeDocComment(&existing, generated)
		if changed {
			t.Fatalf("expected mergeDocComment to be a no-op for whitespace-equivalent line, got %v", existing)
		}
		got := extractItems(existing)
		if len(got) != 1 || got[0] != " //   hello" {
			t.Fatalf("expected existing's raw form preserved, got %v", existing)
		}
	})

	t.Run("block comment dedup", func(t *testing.T) {
		existing := dst.Decorations{"/* foo */", "\n"}
		generated := dst.Decorations{"/* foo */"}
		changed := mergeDocComment(&existing, generated)
		if changed {
			t.Fatal("expected mergeDocComment to be a no-op for duplicate block comment")
		}
		got := extractItems(existing)
		if len(got) != 1 || got[0] != "/* foo */" {
			t.Fatalf("expected existing unchanged, got %v", existing)
		}
	})

	t.Run("block comment added", func(t *testing.T) {
		existing := dst.Decorations{}
		generated := dst.Decorations{"/* foo */"}
		changed := mergeDocComment(&existing, generated)
		if !changed {
			t.Fatal("expected mergeDocComment to report change")
		}
		got := extractItems(existing)
		if len(got) != 1 || got[0] != "/* foo */" {
			t.Fatalf("expected block comment adopted, got %v", existing)
		}
	})

	t.Run("empty paragraph break is deduped", func(t *testing.T) {
		// Two empty // lines collapse — they normalize to "" and dedup.
		existing := dst.Decorations{"// c1", "\n", "//", "\n"}
		generated := dst.Decorations{"// c1", "\n", "//", "\n", "// c2"}
		changed := mergeDocComment(&existing, generated)
		if !changed {
			t.Fatal("expected mergeDocComment to report change")
		}
		got := extractItems(existing)
		want := []string{"// c1", "//", "// c2"}
		if !equalStringSlices(got, want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("empty existing and empty generated stays empty", func(t *testing.T) {
		existing := dst.Decorations{}
		generated := dst.Decorations{}
		changed := mergeDocComment(&existing, generated)
		if changed {
			t.Fatal("expected mergeDocComment to be a no-op")
		}
		if len(existing) != 0 {
			t.Fatalf("expected existing to remain empty, got %v", existing)
		}
	})

	t.Run("only newlines counts as empty existing", func(t *testing.T) {
		existing := dst.Decorations{"\n"}
		generated := dst.Decorations{"// generated doc"}
		changed := mergeDocComment(&existing, generated)
		if !changed {
			t.Fatal("expected mergeDocComment to report change")
		}
		got := extractItems(existing)
		if len(got) != 1 || got[0] != "// generated doc" {
			t.Fatalf("expected existing to be adopted, got %v", existing)
		}
	})

	t.Run("nil existing pointer is a no-op", func(t *testing.T) {
		// Should not panic.
		changed := mergeDocComment(nil, dst.Decorations{"// x"})
		if changed {
			t.Fatal("expected mergeDocComment(nil, ...) to be no-op")
		}
	})

	t.Run("copy is independent", func(t *testing.T) {
		existing := dst.Decorations{}
		generated := dst.Decorations{"// generated doc"}
		mergeDocComment(&existing, generated)
		// Mutating generated must not affect existing.
		generated[0] = "// mutated"
		got := extractItems(existing)
		if len(got) != 1 || got[0] != "// generated doc" {
			t.Fatalf("mutation leaked across copy: existing=%v", existing)
		}
	})
}

// equalStringSlices reports element-wise equality of two []string values.
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFindContainingGenDecl(t *testing.T) {
	t.Run("single type declaration", func(t *testing.T) {
		src := `package p

// doc for Foo
type Foo struct {
	A int
}
`
		f := parseFile(t, src)
		gd := f.Decls[0].(*dst.GenDecl)
		ts := gd.Specs[0].(*dst.TypeSpec)
		got := findContainingGenDecl(f, ts)
		if got != gd {
			t.Fatalf("expected to find containing GenDecl, got %v", got)
		}
	})

	t.Run("grouped type declaration", func(t *testing.T) {
		src := `package p

type (
	Foo struct{ A int }
	Bar struct{ B int }
)
`
		f := parseFile(t, src)
		gd := f.Decls[0].(*dst.GenDecl)
		barSpec := gd.Specs[1].(*dst.TypeSpec)
		got := findContainingGenDecl(f, barSpec)
		if got != gd {
			t.Fatalf("expected to find containing GenDecl for Bar, got %v", got)
		}
	})

	t.Run("typespec not in file returns nil", func(t *testing.T) {
		src := `package p
type Foo struct{ A int }
`
		f := parseFile(t, src)
		other := &dst.TypeSpec{Name: dst.NewIdent("Bogus")}
		if got := findContainingGenDecl(f, other); got != nil {
			t.Fatalf("expected nil for unknown TypeSpec, got %v", got)
		}
	})

	t.Run("nil arguments", func(t *testing.T) {
		if got := findContainingGenDecl(nil, nil); got != nil {
			t.Fatalf("expected nil, got %v", got)
		}
	})
}

func TestFindFieldByName(t *testing.T) {
	src := `package p
type T struct {
	A int
	B string
	C int
}
`
	f := parseFile(t, src)
	gd := f.Decls[0].(*dst.GenDecl)
	ts := gd.Specs[0].(*dst.TypeSpec)
	st := ts.Type.(*dst.StructType)

	if got := findFieldByName(st.Fields, "B"); got == nil || got.Names[0].Name != "B" {
		t.Fatalf("expected to find field B, got %v", got)
	}
	if got := findFieldByName(st.Fields, "Missing"); got != nil {
		t.Fatalf("expected nil for missing field, got %v", got)
	}
	if got := findFieldByName(nil, "A"); got != nil {
		t.Fatalf("expected nil for nil FieldList, got %v", got)
	}
}

func TestFindFieldByEmbedKey(t *testing.T) {
	src := `package p
import "io"
type I interface {
	io.Reader
	io.Closer
	Do() error
}
`
	f := parseFile(t, src)
	gd := f.Decls[1].(*dst.GenDecl) // import is decl[0]
	ts := gd.Specs[0].(*dst.TypeSpec)
	iface := ts.Type.(*dst.InterfaceType)

	if got := findFieldByEmbedKey(iface.Methods, "io.Reader"); got == nil {
		t.Fatal("expected to find io.Reader embed")
	}
	if got := findFieldByEmbedKey(iface.Methods, "io.Writer"); got != nil {
		t.Fatalf("expected nil for missing embed, got %v", got)
	}
	// Named methods must never be returned by an embed lookup.
	if got := findFieldByEmbedKey(iface.Methods, "Do"); got != nil {
		t.Fatalf("expected nil for named method, got %v", got)
	}
}

// --- Integration tests: doc merging through Merge() ---

// TestMergeDocs_FuncAdoptsGeneratedDocWhenMissing verifies that when an existing
// function has no doc comment, the generated function's doc is adopted.
func TestMergeDocs_FuncAdoptsGeneratedDocWhenMissing(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_docs_func_adopt")

	existingSrc := `package merge_docs_func_adopt

func Greet(name string) string {
	return "hello"
}
`
	if err := os.WriteFile(filepath.Join(dir, "greet.go"), []byte(existingSrc), 0o644); err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "greet.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	generatedSrc := `package merge_docs_func_adopt

// Greet returns a greeting for the given name.
func Greet(name string) string {
	return "hello"
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a merged file, got none")
	}

	fd := findFuncDecl(resultFiles[0], "Greet")
	if fd == nil {
		t.Fatal("Greet function not found in result file")
	}
	if !hasDocComment(fd.Decs.Start) {
		t.Fatalf("expected doc comment on Greet, got %v", fd.Decs.Start)
	}
}

// TestMergeDocs_FuncAppendsGeneratedToManualDoc verifies that when an existing
// function already has a doc comment (manual) and the generated function has
// its own (different) doc, the line-level merger appends the generated line(s)
// rather than overwriting the manual ones. Both must be present in the result.
func TestMergeDocs_FuncAppendsGeneratedToManualDoc(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_docs_func_append")

	existingSrc := `package merge_docs_func_append

// Greet says hi in a friendly, customized way.
func Greet(name string) string {
	return "hello"
}
`
	if err := os.WriteFile(filepath.Join(dir, "greet.go"), []byte(existingSrc), 0o644); err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "greet.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	generatedSrc := `package merge_docs_func_append

// Greet returns a generic greeting (auto-generated description).
func Greet(name string) string {
	return "hello"
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a merged file, got none")
	}

	fd := findFuncDecl(resultFiles[0], "Greet")
	if fd == nil {
		t.Fatal("Greet function not found")
	}
	if !containsCommentSubstring(fd.Decs.Start, "friendly, customized") {
		t.Fatalf("expected manual doc preserved, got %v", fd.Decs.Start)
	}
	if !containsCommentSubstring(fd.Decs.Start, "auto-generated") {
		t.Fatalf("expected generated doc appended, got %v", fd.Decs.Start)
	}
}

// TestMergeDocs_Func_LineLevelInterleave verifies that when both the existing
// and generated docs share a common opening line, a generated line that does
// NOT exist in the result is inserted right after the last common anchor —
// preserving any trailing manual-only lines that follow.
//
// existing = ["// Greet does the thing.", "//", "// Manual: extra note."]
// generated = ["// Greet does the thing.", "//", "// Returns nil on success."]
// want order: "Greet does the thing." → "" → "Returns nil on success." → "Manual: extra note."
func TestMergeDocs_Func_LineLevelInterleave(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_docs_func_interleave")

	existingSrc := `package merge_docs_func_interleave

// Greet does the thing.
//
// Manual: extra note.
func Greet(name string) string {
	return "hello"
}
`
	if err := os.WriteFile(filepath.Join(dir, "greet.go"), []byte(existingSrc), 0o644); err != nil {
		t.Fatal(err)
	}

	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "greet.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	generatedSrc := `package merge_docs_func_interleave

// Greet does the thing.
//
// Returns nil on success.
func Greet(name string) string {
	return "hello"
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a merged file, got none")
	}

	fd := findFuncDecl(resultFiles[0], "Greet")
	if fd == nil {
		t.Fatal("Greet function not found")
	}

	// Extract comment items in order.
	var items []string
	for _, s := range fd.Decs.Start {
		ts := strings.TrimSpace(s)
		if strings.HasPrefix(ts, "//") || strings.HasPrefix(ts, "/*") {
			items = append(items, ts)
		}
	}

	// Locate the indices of the three substrings of interest.
	idxThing := indexOfContaining(items, "does the thing")
	idxReturns := indexOfContaining(items, "Returns nil on success")
	idxManual := indexOfContaining(items, "Manual: extra note")

	if idxThing == -1 {
		t.Fatalf("expected shared anchor line present, got %v", items)
	}
	if idxReturns == -1 {
		t.Fatalf("expected generated line appended into doc, got %v", items)
	}
	if idxManual == -1 {
		t.Fatalf("expected manual line preserved, got %v", items)
	}
	if !(idxThing < idxReturns && idxReturns < idxManual) {
		t.Fatalf("expected order anchor < generated-insert < manual-trailing, got %v (idxs: %d, %d, %d)", items, idxThing, idxReturns, idxManual)
	}
}

// indexOfContaining returns the index of the first item containing substr, or -1.
func indexOfContaining(items []string, substr string) int {
	for i, s := range items {
		if strings.Contains(s, substr) {
			return i
		}
	}
	return -1
}

// TestMergeDocs_StructTypeAdoptsAndPreserves covers both adopt and preserve
// scenarios for the type-level doc on a struct declaration. The "preserve"
// subtest now asserts that the generated line is appended alongside the
// manual one (line-level merge).
func TestMergeDocs_StructTypeAdoptsAndPreserves(t *testing.T) {
	t.Run("adopts when existing has no doc", func(t *testing.T) {
		dir := mergeTestFixtureDir(t, "merge_docs_struct_adopt")

		existingSrc := `package merge_docs_struct_adopt

type Container struct {
	Name string
}
`
		if err := os.WriteFile(filepath.Join(dir, "c.go"), []byte(existingSrc), 0o644); err != nil {
			t.Fatal(err)
		}
		pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "c.go")}, dir)
		if err != nil {
			t.Fatalf("inspect.Inspect failed: %v", err)
		}
		pkg := pkgs[0]

		generatedSrc := `package merge_docs_struct_adopt

// Container holds the application state.
type Container struct {
	Name string
}
`
		generatedFile, err := decorator.Parse(generatedSrc)
		if err != nil {
			t.Fatalf("failed to parse generated src: %v", err)
		}

		resultFiles, err := Merge(pkg, generatedFile, "")
		if err != nil {
			t.Fatalf("Merge() error = %v", err)
		}
		if len(resultFiles) == 0 {
			t.Fatal("expected a merged file, got none")
		}

		gd := findGenDeclForType(t, resultFiles[0], "Container")
		if !hasDocComment(gd.Decs.Start) {
			t.Fatalf("expected doc on Container GenDecl, got %v", gd.Decs.Start)
		}
		if !containsCommentSubstring(gd.Decs.Start, "application state") {
			t.Fatalf("expected generated doc adopted, got %v", gd.Decs.Start)
		}
	})

	t.Run("appends generated to manual doc", func(t *testing.T) {
		dir := mergeTestFixtureDir(t, "merge_docs_struct_append")

		existingSrc := `package merge_docs_struct_append

// Container — hand-written description that must survive merges.
type Container struct {
	Name string
}
`
		if err := os.WriteFile(filepath.Join(dir, "c.go"), []byte(existingSrc), 0o644); err != nil {
			t.Fatal(err)
		}
		pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "c.go")}, dir)
		if err != nil {
			t.Fatalf("inspect.Inspect failed: %v", err)
		}
		pkg := pkgs[0]

		generatedSrc := `package merge_docs_struct_append

// Container is a generated description.
type Container struct {
	Name string
}
`
		generatedFile, err := decorator.Parse(generatedSrc)
		if err != nil {
			t.Fatalf("failed to parse generated src: %v", err)
		}

		resultFiles, err := Merge(pkg, generatedFile, "")
		if err != nil {
			t.Fatalf("Merge() error = %v", err)
		}
		if len(resultFiles) == 0 {
			t.Fatal("expected a merged file, got none")
		}

		gd := findGenDeclForType(t, resultFiles[0], "Container")
		if !containsCommentSubstring(gd.Decs.Start, "hand-written") {
			t.Fatalf("expected manual doc preserved, got %v", gd.Decs.Start)
		}
		if !containsCommentSubstring(gd.Decs.Start, "generated description") {
			t.Fatalf("expected generated doc appended, got %v", gd.Decs.Start)
		}
	})
}

// TestMergeDocs_InterfaceTypeAdoptsAndPreserves covers the same logic for
// interface declarations.
func TestMergeDocs_InterfaceTypeAdoptsAndPreserves(t *testing.T) {
	t.Run("adopts when existing has no doc", func(t *testing.T) {
		dir := mergeTestFixtureDir(t, "merge_docs_iface_adopt")

		existingSrc := `package merge_docs_iface_adopt

type Service interface {
	Start() error
}
`
		if err := os.WriteFile(filepath.Join(dir, "s.go"), []byte(existingSrc), 0o644); err != nil {
			t.Fatal(err)
		}
		pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "s.go")}, dir)
		if err != nil {
			t.Fatalf("inspect.Inspect failed: %v", err)
		}
		pkg := pkgs[0]

		generatedSrc := `package merge_docs_iface_adopt

// Service is the lifecycle interface for application services.
type Service interface {
	Start() error
}
`
		generatedFile, err := decorator.Parse(generatedSrc)
		if err != nil {
			t.Fatalf("failed to parse generated src: %v", err)
		}

		resultFiles, err := Merge(pkg, generatedFile, "")
		if err != nil {
			t.Fatalf("Merge() error = %v", err)
		}
		if len(resultFiles) == 0 {
			t.Fatal("expected a merged file, got none")
		}

		gd := findGenDeclForType(t, resultFiles[0], "Service")
		if !containsCommentSubstring(gd.Decs.Start, "lifecycle interface") {
			t.Fatalf("expected generated doc adopted on Service, got %v", gd.Decs.Start)
		}
	})

	t.Run("appends generated to manual doc", func(t *testing.T) {
		dir := mergeTestFixtureDir(t, "merge_docs_iface_append")

		existingSrc := `package merge_docs_iface_append

// Service: my notes on this interface.
type Service interface {
	Start() error
}
`
		if err := os.WriteFile(filepath.Join(dir, "s.go"), []byte(existingSrc), 0o644); err != nil {
			t.Fatal(err)
		}
		pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "s.go")}, dir)
		if err != nil {
			t.Fatalf("inspect.Inspect failed: %v", err)
		}
		pkg := pkgs[0]

		generatedSrc := `package merge_docs_iface_append

// Service is generated.
type Service interface {
	Start() error
}
`
		generatedFile, err := decorator.Parse(generatedSrc)
		if err != nil {
			t.Fatalf("failed to parse generated src: %v", err)
		}

		resultFiles, err := Merge(pkg, generatedFile, "")
		if err != nil {
			t.Fatalf("Merge() error = %v", err)
		}
		if len(resultFiles) == 0 {
			t.Fatal("expected a merged file, got none")
		}

		gd := findGenDeclForType(t, resultFiles[0], "Service")
		if !containsCommentSubstring(gd.Decs.Start, "my notes") {
			t.Fatalf("expected manual doc preserved on Service, got %v", gd.Decs.Start)
		}
		if !containsCommentSubstring(gd.Decs.Start, "is generated") {
			t.Fatalf("expected generated doc appended on Service, got %v", gd.Decs.Start)
		}
	})
}

// TestMergeDocs_InterfaceMethod_AdoptsDocWhenExistingHasNone verifies that
// for a method already present in the existing interface, the generated doc
// is adopted when the existing method has no doc.
func TestMergeDocs_InterfaceMethod_AdoptsDocWhenExistingHasNone(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_docs_iface_method_adopt")

	existingSrc := `package merge_docs_iface_method_adopt

type Service interface {
	Start() error
}
`
	if err := os.WriteFile(filepath.Join(dir, "s.go"), []byte(existingSrc), 0o644); err != nil {
		t.Fatal(err)
	}
	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "s.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	generatedSrc := `package merge_docs_iface_method_adopt

type Service interface {
	// Start launches the service in the background.
	Start() error
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a merged file, got none")
	}

	iface := findInterfaceType(t, resultFiles[0], "Service")
	startField := findFieldByName(iface.Methods, "Start")
	if startField == nil {
		t.Fatal("Start method not found")
	}
	if !containsCommentSubstring(startField.Decs.Start, "launches the service") {
		t.Fatalf("expected generated doc adopted on Start method, got %v", startField.Decs.Start)
	}
}

// TestMergeDocs_InterfaceMethod_AppendsGeneratedToManualDoc verifies that a
// hand-written doc on an existing interface method is preserved AND that the
// generated method's doc line is appended alongside it (line-level merge).
func TestMergeDocs_InterfaceMethod_AppendsGeneratedToManualDoc(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_docs_iface_method_append")

	existingSrc := `package merge_docs_iface_method_append

type Service interface {
	// Start: manual note about Start semantics.
	Start() error
}
`
	if err := os.WriteFile(filepath.Join(dir, "s.go"), []byte(existingSrc), 0o644); err != nil {
		t.Fatal(err)
	}
	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "s.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	generatedSrc := `package merge_docs_iface_method_append

type Service interface {
	// Start launches the service (generated).
	Start() error
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a merged file, got none")
	}

	iface := findInterfaceType(t, resultFiles[0], "Service")
	startField := findFieldByName(iface.Methods, "Start")
	if startField == nil {
		t.Fatal("Start method not found")
	}
	if !containsCommentSubstring(startField.Decs.Start, "manual note") {
		t.Fatalf("expected manual doc preserved on Start, got %v", startField.Decs.Start)
	}
	if !containsCommentSubstring(startField.Decs.Start, "(generated)") {
		t.Fatalf("expected generated doc appended on Start, got %v", startField.Decs.Start)
	}
}

// TestMergeDocs_StructField_AdoptsAndPreserves verifies field-level doc merging
// on struct fields and ensures inline trailing comments are not touched.
func TestMergeDocs_StructField_AdoptsAndPreserves(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_docs_struct_field")

	existingSrc := `package merge_docs_struct_field

type Container struct {
	Name string
	// Count: manual description that must remain.
	Count int
	Note  string // inline note preserved
}
`
	if err := os.WriteFile(filepath.Join(dir, "c.go"), []byte(existingSrc), 0o644); err != nil {
		t.Fatal(err)
	}
	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "c.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	// Generated provides docs for Name (existing has none — should adopt) and
	// for Count (existing has manual — must be preserved).
	generatedSrc := `package merge_docs_struct_field

type Container struct {
	// Name is the human-readable identifier.
	Name string
	// Count is the generated description for Count.
	Count int
	// Note is the generated description (must NOT overwrite inline note).
	Note string
}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a merged file, got none")
	}

	// Locate Container's struct type in the result.
	gd := findGenDeclForType(t, resultFiles[0], "Container")
	ts := gd.Specs[0].(*dst.TypeSpec)
	st := ts.Type.(*dst.StructType)

	nameField := findFieldByName(st.Fields, "Name")
	if nameField == nil {
		t.Fatal("Name field not found")
	}
	if !containsCommentSubstring(nameField.Decs.Start, "human-readable identifier") {
		t.Fatalf("expected generated doc adopted on Name, got %v", nameField.Decs.Start)
	}

	countField := findFieldByName(st.Fields, "Count")
	if countField == nil {
		t.Fatal("Count field not found")
	}
	if !containsCommentSubstring(countField.Decs.Start, "manual description") {
		t.Fatalf("expected manual doc preserved on Count, got %v", countField.Decs.Start)
	}
	if !containsCommentSubstring(countField.Decs.Start, "generated description for Count") {
		t.Fatalf("expected generated doc appended on Count, got %v", countField.Decs.Start)
	}

	// The inline note on Note must be preserved untouched (Decs.End).
	noteField := findFieldByName(st.Fields, "Note")
	if noteField == nil {
		t.Fatal("Note field not found")
	}
	if !containsCommentSubstring(noteField.Decs.End, "inline note preserved") {
		t.Fatalf("expected inline note preserved on Note.Decs.End, got %v", noteField.Decs.End)
	}
}

// TestMergeDocs_NewEntitiesStillCarryGeneratedDocs is a sanity check verifying
// that entirely-new declarations added by Merge still carry the generated doc
// (this code path is untouched by the doc-merge changes but the test guards
// against regressions in the surrounding refactor).
func TestMergeDocs_NewEntitiesStillCarryGeneratedDocs(t *testing.T) {
	dir := mergeTestFixtureDir(t, "merge_docs_new_entity")

	existingSrc := `package merge_docs_new_entity

type Existing struct{}
`
	if err := os.WriteFile(filepath.Join(dir, "e.go"), []byte(existingSrc), 0o644); err != nil {
		t.Fatal(err)
	}
	pkgs, err := inspect.Inspect([]string{filepath.Join(dir, "e.go")}, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	pkg := pkgs[0]

	generatedSrc := `package merge_docs_new_entity

// NewType is a freshly-added type with its generated doc.
type NewType struct {
	A int
}

// NewFunc is a freshly-added function with its generated doc.
func NewFunc() {}
`
	generatedFile, err := decorator.Parse(generatedSrc)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}

	resultFiles, err := Merge(pkg, generatedFile, "e.go")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(resultFiles) == 0 {
		t.Fatal("expected a merged file, got none")
	}

	gd := findGenDeclForType(t, resultFiles[0], "NewType")
	if !containsCommentSubstring(gd.Decs.Start, "freshly-added type") {
		t.Fatalf("expected generated doc on new type, got %v", gd.Decs.Start)
	}

	fd := findFuncDecl(resultFiles[0], "NewFunc")
	if fd == nil {
		t.Fatal("NewFunc not found")
	}
	if !containsCommentSubstring(fd.Decs.Start, "freshly-added function") {
		t.Fatalf("expected generated doc on new function, got %v", fd.Decs.Start)
	}
}

// --- Doc test helpers ---

// containsCommentSubstring reports whether any decoration string contains the
// given substring (after trimming the leading // or /* prefix). Used by doc
// merge tests to assert on rendered comment text without depending on exact
// formatting decisions made by the decorator.
func containsCommentSubstring(decs dst.Decorations, substr string) bool {
	for _, s := range decs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

// findGenDeclForType locates the *dst.GenDecl in a file whose Specs contain a
// TypeSpec with the given name. It is used by doc-merge tests to assert on
// type-level doc comments (which live on the containing GenDecl).
func findGenDeclForType(t *testing.T, file *dst.File, name string) *dst.GenDecl {
	t.Helper()
	for _, decl := range file.Decls {
		gd, ok := decl.(*dst.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*dst.TypeSpec)
			if !ok {
				continue
			}
			if ts.Name != nil && ts.Name.Name == name {
				return gd
			}
		}
	}
	t.Fatalf("GenDecl for type %q not found in file", name)
	return nil
}
