// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains tests for file target parsing and transformation filtering logic.

package shape

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/query/model"
	"gotest.tools/v3/assert"
)

func TestParseFileTargets(t *testing.T) {
	cwd, err := os.Getwd()
	assert.NilError(t, err)

	tests := []struct {
		name    string
		args    []string
		want    []FileTarget
		wantErr string
	}{
		{
			name: "empty args",
			args: []string{},
			want: nil,
		},
		{
			name: "file path without line number",
			args: []string{"./dir/file.go"},
			want: []FileTarget{
				{Path: filepath.Join(cwd, path.Join("dir", "file.go")), Line: 0},
			},
		},
		{
			name: "file path with line number",
			args: []string{"./dir/file.go:30"},
			want: []FileTarget{
				{Path: filepath.Join(cwd, path.Join("dir", "file.go")), Line: 30},
			},
		},
		{
			name: "multiple targets",
			args: []string{"./a.go:10", "./b.go"},
			want: []FileTarget{
				{Path: filepath.Join(cwd, "a.go"), Line: 10},
				{Path: filepath.Join(cwd, "b.go"), Line: 0},
			},
		},
		{
			name:    "zero line number",
			args:    []string{"./file.go:0"},
			wantErr: "line number must be positive",
		},
		{
			name:    "negative line number",
			args:    []string{"./file.go:-5"},
			wantErr: "line number must be positive",
		},
		{
			name: "non-numeric suffix treated as path",
			args: []string{"./file.go:abc"},
			want: []FileTarget{
				{Path: filepath.Join(cwd, "file.go:abc"), Line: 0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFileTargets(tt.args)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NilError(t, err)
			assert.DeepEqual(t, tt.want, got)
		})
	}
}

// testNode is a minimal model.Node implementation for testing.
type testNode struct {
	pkg         *model.Package
	position    model.Position
	doc         string
	annotations model.Annotations
}

func (n *testNode) GetAnnotations() model.Annotations { return n.annotations }
func (n *testNode) GetPackage() *model.Package        { return n.pkg }
func (n *testNode) GetPosition() model.Position       { return n.position }
func (n *testNode) GetDoc() string                    { return n.doc }

// stubTransformer is a minimal Transformer for testing. It only implements enough
// to be used as a key in dedup comparisons.
type stubTransformer struct {
	BasicTransformer
}

func (s *stubTransformer) Render(_, _, _, _, _ interface{}) (string, error) {
	return "", nil
}

func newStubTransformer(name string) Transformer {
	return &Shaper{BasicTransformer: BasicTransformer{Name: name}}
}

func TestFilterTransformations_EmptyTargets(t *testing.T) {
	node := &testNode{position: model.Position{Filename: "/a.go", Line: 10}}
	ts := []Transformation{{Node: node, Transformer: newStubTransformer("t1")}}

	result, err := filterTransformations(ts, nil)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
}

func TestFilterTransformations_FileOnly(t *testing.T) {
	node1 := &testNode{position: model.Position{Filename: "/a.go", Line: 10}}
	node2 := &testNode{position: model.Position{Filename: "/b.go", Line: 20}}
	t1 := newStubTransformer("t1")
	t2 := newStubTransformer("t2")

	ts := []Transformation{
		{Node: node1, Transformer: t1},
		{Node: node2, Transformer: t2},
	}

	result, err := filterTransformations(ts, []FileTarget{{Path: "/a.go"}})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Node.GetPosition().Filename, "/a.go")
}

func TestFilterTransformations_LineHitsDeclaration(t *testing.T) {
	// Node at line 10, with doc comment from lines 7-9.
	// Line 10 is the declaration — should return all transformations.
	node := &testNode{
		position: model.Position{Filename: "/a.go", Line: 10},
		doc:      "plumber:derive\nplumber:template foo\nplumber:shape\n",
		annotations: model.Annotations{
			{Name: contract.TransformationDerive, DocLine: 1},
			{Name: contract.OptionTemplate, DocLine: 2},
			{Name: contract.TransformationShape, DocLine: 3},
		},
	}
	t1 := newStubTransformer("derive")
	t2 := newStubTransformer("shape")

	ts := []Transformation{
		{Node: node, Transformer: t1},
		{Node: node, Transformer: t2},
	}

	result, err := filterTransformations(ts, []FileTarget{{Path: "/a.go", Line: 10}})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 2)
}

func TestFilterTransformations_LineHitsTransformerAnnotation(t *testing.T) {
	// Doc comment:
	//   line 7: plumber:derive
	//   line 8: plumber:template foo
	//   line 9: plumber:shape
	// Declaration at line 10.
	node := &testNode{
		position: model.Position{Filename: "/a.go", Line: 10},
		doc:      "plumber:derive\nplumber:template foo\nplumber:shape\n",
		annotations: model.Annotations{
			{Name: contract.TransformationDerive, DocLine: 1},
			{Name: contract.OptionTemplate, DocLine: 2},
			{Name: contract.TransformationShape, DocLine: 3},
		},
	}
	tDerive := newStubTransformer("derive")
	tShape := newStubTransformer("shape")

	ts := []Transformation{
		{Node: node, Transformer: tDerive},
		{Node: node, Transformer: tShape},
	}

	// Point to the plumber:shape annotation (line 9 = nodeStart + DocLine 3 - 1 = 7 + 3 - 1 = 9)
	result, err := filterTransformations(ts, []FileTarget{{Path: "/a.go", Line: 9}})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Transformer.GetName(), "shape")

	// Point to the plumber:derive annotation (line 7 = nodeStart + 1 - 1 = 7)
	result, err = filterTransformations(ts, []FileTarget{{Path: "/a.go", Line: 7}})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Transformer.GetName(), "derive")
}

func TestFilterTransformations_LineHitsModifierAnnotation(t *testing.T) {
	// Doc comment:
	//   line 7: plumber:derive
	//   line 8: plumber:template foo
	//   line 9: plumber:shape
	// Declaration at line 10.
	node := &testNode{
		position: model.Position{Filename: "/a.go", Line: 10},
		doc:      "plumber:derive\nplumber:template foo\nplumber:shape\n",
		annotations: model.Annotations{
			{Name: contract.TransformationDerive, DocLine: 1},
			{Name: contract.OptionTemplate, DocLine: 2},
			{Name: contract.TransformationShape, DocLine: 3},
		},
	}
	tDerive := newStubTransformer("derive")
	tShape := newStubTransformer("shape")

	ts := []Transformation{
		{Node: node, Transformer: tDerive},
		{Node: node, Transformer: tShape},
	}

	// Point to plumber:template (line 8), which is a modifier under plumber:derive.
	// Should return the derive transformation (nearest transformer above).
	result, err := filterTransformations(ts, []FileTarget{{Path: "/a.go", Line: 8}})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Transformer.GetName(), "derive")
}

func TestFilterTransformations_LineNoMatch(t *testing.T) {
	node := &testNode{
		position:    model.Position{Filename: "/a.go", Line: 10},
		doc:         "plumber:derive\n",
		annotations: model.Annotations{{Name: contract.TransformationDerive, DocLine: 1}},
	}
	ts := []Transformation{{Node: node, Transformer: newStubTransformer("derive")}}

	// Line 50 doesn't match any node.
	_, err := filterTransformations(ts, []FileTarget{{Path: "/a.go", Line: 50}})
	assert.ErrorContains(t, err, "does not match any annotated node")
}

func TestFilterTransformations_Dedup(t *testing.T) {
	node := &testNode{position: model.Position{Filename: "/a.go", Line: 10}}
	tr := newStubTransformer("t1")
	ts := []Transformation{{Node: node, Transformer: tr}}

	// Two targets pointing to the same file — should not duplicate.
	result, err := filterTransformations(ts, []FileTarget{
		{Path: "/a.go"},
		{Path: "/a.go"},
	})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
}
