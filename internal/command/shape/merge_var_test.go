// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for inplace merging of package-level variable declarations, covering
// exported and unexported variables, generic types and initializers, grouped and multi-name
// declarations, doc-comment reconciliation, and idempotency across repeated runs.

package shape

import (
	"bytes"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/dave/dst/decorator/resolver/goast"
	"github.com/dave/dst/decorator/resolver/gopackages"
	"github.com/getoutreach/plumber/internal/astx/inspect"
	"github.com/getoutreach/plumber/query/model"
)

// --- var test helpers ---

// countVarSpecs returns how many top-level var declarations named name exist across
// all files of the package.
func countVarSpecs(pkg *model.Package, name string) int {
	n := 0
	for _, f := range pkg.Package.Syntax {
		for _, decl := range f.Decls {
			gd, ok := decl.(*dst.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*dst.ValueSpec)
				if !ok {
					continue
				}
				for _, ident := range vs.Names {
					if ident != nil && ident.Name == name {
						n++
					}
				}
			}
		}
	}
	return n
}

// inspectVarFixture writes files into a fresh fixture directory and inspects them.
func inspectVarFixture(t *testing.T, name string, files map[string]string) (*model.Package, string) {
	t.Helper()
	dir := mergeTestFixtureDir(t, name)
	paths := make([]string, 0, len(files))
	for fn, src := range files {
		p := filepath.Join(dir, fn)
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	pkgs, err := inspect.Inspect(paths, dir)
	if err != nil {
		t.Fatalf("inspect.Inspect failed: %v", err)
	}
	if len(pkgs) == 0 {
		t.Fatal("expected at least one package")
	}
	return pkgs[0], dir
}

// parseGeneratedLikeInplace parses generated source the same way InplaceManager does,
// resolving package selectors to Ident.Path via the goast resolver.
func parseGeneratedLikeInplace(t *testing.T, pkg *model.Package, src string) *dst.File {
	t.Helper()
	dec := decorator.NewDecoratorWithImports(nil, pkg.Package.PkgPath, goast.New())
	f, err := dec.Parse(src)
	if err != nil {
		t.Fatalf("failed to parse generated src: %v", err)
	}
	return f
}

// restoreFiles writes each merged dst.File back to disk using the same restorer
// configuration as restoreOutput, and returns the rendered sources keyed by filename.
func restoreFiles(t *testing.T, pkg *model.Package, dir string, files []*dst.File) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, f := range files {
		filename := pkg.Package.Decorator.Filenames[f]
		var buf bytes.Buffer
		r := decorator.NewRestorerWithImports(pkg.Package.PkgPath, gopackages.New(dir))
		if err := r.Fprint(&buf, f); err != nil {
			t.Fatalf("restore %q: %v", filename, err)
		}
		if err := os.WriteFile(filename, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		out[filepath.Base(filename)] = buf.String()
	}
	return out
}

// --- tests ---

// TestMergeVar_AddsNewVar verifies that a missing variable is appended to the output
// file as a standalone var declaration carrying the generated doc comment.
func TestMergeVar_AddsNewVar(t *testing.T) {
	pkg, _ := inspectVarFixture(t, "merge_var_add", map[string]string{
		"marker.go": "package merge_var_add\n\ntype Marker struct{}\n",
	})

	generated := parseFile(t, `package merge_var_add

// variableName is generated.
var variableName string = string("v")
`)
	files, err := Merge(pkg, generated, "vars.go")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 merged file, got %d", len(files))
	}
	if got := countVarSpecs(pkg, "variableName"); got != 1 {
		t.Fatalf("expected exactly one variableName declaration, got %d", got)
	}
	gd := files[0].Decls[len(files[0].Decls)-1].(*dst.GenDecl)
	if !containsCommentSubstring(gd.Decs.Start, "variableName is generated") {
		t.Fatalf("expected generated doc on new var, got %v", gd.Decs.Start)
	}
}

// TestMergeVar_SkipsExistingUnexported reproduces the reported bug: an unexported
// variable is not recorded in pkg.Vars by the inspect pipeline, so it used to be
// appended again on every run.
func TestMergeVar_SkipsExistingUnexported(t *testing.T) {
	pkg, _ := inspectVarFixture(t, "merge_var_skip_unexported", map[string]string{
		"vars.go": "package merge_var_skip_unexported\n\nvar variableName string = string(\"v\")\n",
	})

	generated := parseFile(t, `package merge_var_skip_unexported

var variableName string = string("v")
`)
	files, err := Merge(pkg, generated, "vars.go")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected no merged files, got %d", len(files))
	}
	if got := countVarSpecs(pkg, "variableName"); got != 1 {
		t.Fatalf("expected exactly one variableName declaration, got %d", got)
	}
}

// TestMergeVar_SkipsExistingExported verifies exported variables keep being deduplicated.
func TestMergeVar_SkipsExistingExported(t *testing.T) {
	pkg, _ := inspectVarFixture(t, "merge_var_skip_exported", map[string]string{
		"vars.go": "package merge_var_skip_exported\n\nvar VariableName = \"original\"\n",
	})

	generated := parseFile(t, "package merge_var_skip_exported\n\nvar VariableName = \"regenerated\"\n")
	files, err := Merge(pkg, generated, "vars.go")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected no merged files, got %d", len(files))
	}
	if got := countVarSpecs(pkg, "VariableName"); got != 1 {
		t.Fatalf("expected exactly one VariableName declaration, got %d", got)
	}
}

// TestMergeVar_SkipsExistingGeneric covers the reported generic form
// `var userSchema *mapping.Schema[entity.X, postgres.Y] = ...` with type arguments
// coming from imported packages and a generic constructor call as initializer.
func TestMergeVar_SkipsExistingGeneric(t *testing.T) {
	existing := `package merge_var_generic_skip

import (
	"sync/atomic"
	"time"
)

type Schema[K comparable, V any] struct{ m map[K]V }

func NewSchema[K comparable, V any]() *Schema[K, V] { return &Schema[K, V]{m: map[K]V{}} }

var userSchema *Schema[string, time.Duration] = NewSchema[string, time.Duration]()

var durationPtr *atomic.Pointer[time.Duration] = new(atomic.Pointer[time.Duration])
`
	pkg, _ := inspectVarFixture(t, "merge_var_generic_skip", map[string]string{"schema.go": existing})

	generated := parseGeneratedLikeInplace(t, pkg, `package merge_var_generic_skip

import (
	"sync/atomic"
	"time"
)

var userSchema *Schema[string, time.Duration] = NewSchema[string, time.Duration]()

var durationPtr *atomic.Pointer[time.Duration] = new(atomic.Pointer[time.Duration])
`)
	files, err := Merge(pkg, generated, "schema.go")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected no merged files, got %d", len(files))
	}
	for _, name := range []string{"userSchema", "durationPtr"} {
		if got := countVarSpecs(pkg, name); got != 1 {
			t.Fatalf("expected exactly one %s declaration, got %d", name, got)
		}
	}
}

// TestMergeVar_GenericRoundTripIsIdempotent simulates two consecutive plumber runs:
// the first adds generic vars (with imported type args and generic initializers) to a
// new file which is then restored to disk; the second run re-inspects the package (so
// the restored file must type-check) and must not duplicate anything.
func TestMergeVar_GenericRoundTripIsIdempotent(t *testing.T) {
	existing := `package merge_var_generic_rt

type Schema[K comparable, V any] struct{ m map[K]V }

func NewSchema[K comparable, V any]() *Schema[K, V] { return &Schema[K, V]{m: map[K]V{}} }
`
	pkg, dir := inspectVarFixture(t, "merge_var_generic_rt", map[string]string{"schema.go": existing})

	generatedSrc := `package merge_var_generic_rt

import (
	"sync/atomic"
	"time"
)

// userSchema maps users.
var userSchema *Schema[string, time.Duration] = NewSchema[string, time.Duration]()

var durationPtr = &atomic.Pointer[time.Duration]{}

var (
	// grouped is declared in a group.
	grouped, other = NewSchema[int, atomic.Bool](), time.Second
)
`
	files, err := Merge(pkg, parseGeneratedLikeInplace(t, pkg, generatedSrc), "vars_gen.go")
	if err != nil {
		t.Fatalf("first Merge() error = %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 merged file, got %d", len(files))
	}
	rendered := restoreFiles(t, pkg, dir, files)["vars_gen.go"]
	for _, want := range []string{
		`"sync/atomic"`,
		`"time"`,
		"// userSchema maps users.",
		"var userSchema *Schema[string, time.Duration] = NewSchema[string, time.Duration]()",
		"var durationPtr = &atomic.Pointer[time.Duration]{}",
		"// grouped is declared in a group.",
		"var grouped, other = NewSchema[int, atomic.Bool](), time.Second",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("restored file missing %q:\n%s", want, rendered)
		}
	}

	// Second run: re-inspect from disk (fails if the restored file does not compile).
	pkgs, err := inspect.Inspect(
		[]string{filepath.Join(dir, "schema.go"), filepath.Join(dir, "vars_gen.go")}, dir,
	)
	if err != nil {
		t.Fatalf("second inspect failed: %v", err)
	}
	pkg2 := pkgs[0]
	if pkg2.Package.TypesInfo == nil || len(pkg2.Package.Errors) != 0 {
		t.Fatalf("restored package has errors: %v", pkg2.Package.Errors)
	}
	files, err = Merge(pkg2, parseGeneratedLikeInplace(t, pkg2, generatedSrc), "vars_gen.go")
	if err != nil {
		t.Fatalf("second Merge() error = %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected second run to merge nothing, got %d files", len(files))
	}
	for _, name := range []string{"userSchema", "durationPtr", "grouped", "other"} {
		if got := countVarSpecs(pkg2, name); got != 1 {
			t.Fatalf("expected exactly one %s declaration, got %d", name, got)
		}
	}
}

// TestMergeVar_SkipsExistingInGroupedBlock verifies that variables declared inside an
// existing `var ( ... )` block are detected.
func TestMergeVar_SkipsExistingInGroupedBlock(t *testing.T) {
	pkg, _ := inspectVarFixture(t, "merge_var_group_skip", map[string]string{
		"vars.go": "package merge_var_group_skip\n\nvar (\n\tfirst = 1\n\tsecond = 2\n)\n",
	})

	generated := parseFile(t, "package merge_var_group_skip\n\nvar second = 2\n")
	files, err := Merge(pkg, generated, "vars.go")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected no merged files, got %d", len(files))
	}
	if got := countVarSpecs(pkg, "second"); got != 1 {
		t.Fatalf("expected exactly one second declaration, got %d", got)
	}
}

// TestMergeVar_MultiNameSkipsWhenAnyExists verifies multi-name specs are atomic: when
// any declared name exists the spec is skipped and no partial declaration leaks in.
func TestMergeVar_MultiNameSkipsWhenAnyExists(t *testing.T) {
	pkg, _ := inspectVarFixture(t, "merge_var_multi", map[string]string{
		"vars.go": "package merge_var_multi\n\nvar b = 2\n",
	})

	generated := parseFile(t, "package merge_var_multi\n\nvar a, b = 1, 2\n")
	files, err := Merge(pkg, generated, "vars.go")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected no merged files, got %d", len(files))
	}
	if got := countVarSpecs(pkg, "a"); got != 0 {
		t.Fatalf("expected a not to be added, got %d", got)
	}
}

// TestMergeVar_DocAdoptsAndPreserves verifies the preserve-manual doc rule for
// existing variables, both standalone and inside a grouped block.
func TestMergeVar_DocAdoptsAndPreserves(t *testing.T) {
	t.Run("adopts when existing has no doc", func(t *testing.T) {
		pkg, _ := inspectVarFixture(t, "merge_var_doc_adopt", map[string]string{
			"vars.go": "package merge_var_doc_adopt\n\nvar variableName = \"v\"\n",
		})
		generated := parseFile(t, "package merge_var_doc_adopt\n\n// variableName is generated.\nvar variableName = \"v\"\n")
		files, err := Merge(pkg, generated, "vars.go")
		if err != nil {
			t.Fatalf("Merge() error = %v", err)
		}
		if len(files) != 1 {
			t.Fatalf("expected 1 merged file (doc changed), got %d", len(files))
		}
		gd := findTopLevelValueSpec(pkg, token.VAR, "variableName").Decl
		if !containsCommentSubstring(gd.Decs.Start, "variableName is generated") {
			t.Fatalf("expected generated doc adopted, got %v", gd.Decs.Start)
		}
		if got := countVarSpecs(pkg, "variableName"); got != 1 {
			t.Fatalf("expected exactly one declaration, got %d", got)
		}
	})

	t.Run("preserves manual doc in grouped block", func(t *testing.T) {
		pkg, _ := inspectVarFixture(t, "merge_var_doc_group", map[string]string{
			"vars.go": "package merge_var_doc_group\n\nvar (\n\t// hand-written doc.\n\tvariableName = \"v\"\n)\n",
		})
		generated := parseFile(t, "package merge_var_doc_group\n\n// generated doc.\nvar variableName = \"v\"\n")
		if _, err := Merge(pkg, generated, "vars.go"); err != nil {
			t.Fatalf("Merge() error = %v", err)
		}
		spec := findTopLevelValueSpec(pkg, token.VAR, "variableName").Spec
		if !containsCommentSubstring(spec.Decs.Start, "hand-written doc") {
			t.Fatalf("expected manual doc preserved, got %v", spec.Decs.Start)
		}
		if !containsCommentSubstring(spec.Decs.Start, "generated doc") {
			t.Fatalf("expected generated doc merged, got %v", spec.Decs.Start)
		}
	})

	t.Run("no change returns no files", func(t *testing.T) {
		pkg, _ := inspectVarFixture(t, "merge_var_doc_same", map[string]string{
			"vars.go": "package merge_var_doc_same\n\n// same doc.\nvar variableName = \"v\"\n",
		})
		generated := parseFile(t, "package merge_var_doc_same\n\n// same doc.\nvar variableName = \"v\"\n")
		files, err := Merge(pkg, generated, "vars.go")
		if err != nil {
			t.Fatalf("Merge() error = %v", err)
		}
		if len(files) != 0 {
			t.Fatalf("expected no merged files, got %d", len(files))
		}
	})
}

// TestMergeVar_SubsequentMergesInSameRun verifies that an unexported variable added by a
// first Merge() call (into a newly created file) is detected by a second call in the same run.
func TestMergeVar_SubsequentMergesInSameRun(t *testing.T) {
	pkg, _ := inspectVarFixture(t, "merge_var_same_run", map[string]string{
		"marker.go": "package merge_var_same_run\n\ntype Marker struct{}\n",
	})
	src := "package merge_var_same_run\n\nvar variableName string = string(\"v\")\n"

	if _, err := Merge(pkg, parseFile(t, src), "vars.go"); err != nil {
		t.Fatalf("first Merge() error = %v", err)
	}
	files, err := Merge(pkg, parseFile(t, src), "vars.go")
	if err != nil {
		t.Fatalf("second Merge() error = %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected second Merge() to skip, got %d files", len(files))
	}
	if got := countVarSpecs(pkg, "variableName"); got != 1 {
		t.Fatalf("expected exactly one declaration, got %d", got)
	}
}

// TestMergeConst_SkipsExistingUnexported verifies unexported constants, which are not
// recorded in pkg.Consts, are no longer duplicated.
func TestMergeConst_SkipsExistingUnexported(t *testing.T) {
	pkg, _ := inspectVarFixture(t, "merge_const_skip_unexported", map[string]string{
		"consts.go": "package merge_const_skip_unexported\n\nconst (\n\tfoo = iota\n\tbar\n)\n",
	})

	generated := parseFile(t, "package merge_const_skip_unexported\n\nconst bar = 1\n")
	files, err := Merge(pkg, generated, "consts.go")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected no merged files, got %d", len(files))
	}
	n := 0
	for _, f := range pkg.Package.Syntax {
		if findGenDeclForConst(f, "bar") != nil {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("expected exactly one bar const declaration, got %d", n)
	}
}
