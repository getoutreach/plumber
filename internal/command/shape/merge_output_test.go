// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Tests for placing new methods into the explicit plumber:output file during inplace merge.

package shape

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/astx/inspect"
	"github.com/getoutreach/plumber/query/model"
	"gotest.tools/v3/assert"
)

// loadMergeOutputPkg writes the given files into a fixture package and inspects it.
func loadMergeOutputPkg(t *testing.T, name string, files map[string]string) *model.Package {
	t.Helper()
	dir := mergeTestFixtureDir(t, name)
	var paths []string
	for f, src := range files {
		p := filepath.Join(dir, f)
		assert.NilError(t, os.WriteFile(p, []byte(src), 0o600))
		paths = append(paths, p)
	}
	pkgs, err := inspect.Inspect(paths, dir)
	assert.NilError(t, err)
	return pkgs[0]
}

// mergedFilenames returns the base names of the merged files.
func mergedFilenames(pkg *model.Package, files []*dst.File) []string {
	var names []string
	for _, f := range files {
		names = append(names, filepath.Base(pkg.Package.Decorator.Filenames[f]))
	}
	return names
}

const workerSrc = `package mergeout

type Worker struct{ Name string }

func (w *Worker) Existing() string { return w.Name }
`

func TestMergePreferOutput_NewMethodGoesToOutput(t *testing.T) {
	pkg := loadMergeOutputPkg(t, "mergeout_new", map[string]string{"worker.go": workerSrc})
	gen, err := decorator.Parse("package mergeout\n\nfunc (w *Worker) Added() string { return w.Name }\n")
	assert.NilError(t, err)

	files, err := Merge(pkg, gen, "worker_gen.go", MergeOption{PreferOutput: true})
	assert.NilError(t, err)
	assert.DeepEqual(t, mergedFilenames(pkg, files), []string{"worker_gen.go"})
}

func TestMergeDefault_NewMethodGoesToReceiverFile(t *testing.T) {
	pkg := loadMergeOutputPkg(t, "mergeout_default", map[string]string{"worker.go": workerSrc})
	gen, err := decorator.Parse("package mergeout\n\nfunc (w *Worker) Added() string { return w.Name }\n")
	assert.NilError(t, err)

	files, err := Merge(pkg, gen, "generated.go")
	assert.NilError(t, err)
	assert.DeepEqual(t, mergedFilenames(pkg, files), []string{"worker.go"})
}

func TestMergePreferOutput_ExistingMethodStays(t *testing.T) {
	pkg := loadMergeOutputPkg(t, "mergeout_existing", map[string]string{"worker.go": workerSrc})
	gen, err := decorator.Parse("package mergeout\n\nfunc (w *Worker) Existing() string { return w.Name }\n")
	assert.NilError(t, err)

	files, err := Merge(pkg, gen, "worker_gen.go", MergeOption{PreferOutput: true})
	assert.NilError(t, err)
	assert.DeepEqual(t, mergedFilenames(pkg, files), []string{"worker.go"})
}

func TestMergePreferOutput_GeneratedTypeAndMethods(t *testing.T) {
	pkg := loadMergeOutputPkg(t, "mergeout_gentype", map[string]string{"worker.go": workerSrc})
	gen, err := decorator.Parse(`package mergeout

type WorkerFilter struct{ Name string }

func (f *WorkerFilter) Match() bool { return f.Name != "" }
`)
	assert.NilError(t, err)

	files, err := Merge(pkg, gen, "worker_gen.go", MergeOption{PreferOutput: true})
	assert.NilError(t, err)
	assert.DeepEqual(t, mergedFilenames(pkg, files), []string{"worker_gen.go"})
}

func TestMergePreferOutput_UnknownReceiverFails(t *testing.T) {
	pkg := loadMergeOutputPkg(t, "mergeout_unknown", map[string]string{"worker.go": workerSrc})
	gen, err := decorator.Parse("package mergeout\n\nfunc (o *Other) Added() {}\n")
	assert.NilError(t, err)

	_, err = Merge(pkg, gen, "worker_gen.go", MergeOption{PreferOutput: true})
	assert.ErrorContains(t, err, `receiver type is not declared in that package`)
}
