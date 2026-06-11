// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains tests for file target parsing and transformation filtering logic.

package shape

import (
	"os"
	"path"
	"path/filepath"
	"testing"

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

// newTestTransformer creates a Shaper whose Options.Source points to the given node,
// so that matchTarget can resolve the source file and line.
func newTestTransformer(name string, source model.Node, annotations ...model.Annotation) Transformer {
	return &Shaper{BasicTransformer: BasicTransformer{
		Name:        name,
		Options:     model.Annotation{Source: source},
		Annotations: annotations,
	}}
}

func TestFilterTransformations_EmptyTargets(t *testing.T) {
	node := &testNode{position: model.Position{Filename: "/a.go", Line: 10}}
	ts := []Transformation{{Node: node, Transformer: newTestTransformer("t1", node)}}

	result, err := filterTransformations(ts, nil)
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
}

func TestFilterTransformations_FileOnly(t *testing.T) {
	node1 := &testNode{position: model.Position{Filename: "/a.go", Line: 10}}
	node2 := &testNode{position: model.Position{Filename: "/b.go", Line: 20}}

	ts := []Transformation{
		{Node: node1, Transformer: newTestTransformer("t1", node1)},
		{Node: node2, Transformer: newTestTransformer("t2", node2)},
	}

	result, err := filterTransformations(ts, []FileTarget{{Path: "/a.go"}})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Transformer.GetName(), "t1")
}

func TestFilterTransformations_LineHitsDeclaration(t *testing.T) {
	// Source node at line 10. Both transformers point to the same source.
	// Targeting line 10 (the declaration) should match both transformers
	// because their source position line equals the target line.
	node := &testNode{position: model.Position{Filename: "/a.go", Line: 10}}

	ts := []Transformation{
		{Node: node, Transformer: newTestTransformer("derive", node)},
		{Node: node, Transformer: newTestTransformer("shape", node)},
	}

	result, err := filterTransformations(ts, []FileTarget{{Path: "/a.go", Line: 10}})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 2)
}

func TestFilterTransformations_LineHitsAnnotationPosition(t *testing.T) {
	// Source node at line 10. The "shape" transformer has an annotation whose
	// Position is at line 9. Targeting line 9 should match only "shape".
	node := &testNode{position: model.Position{Filename: "/a.go", Line: 10}}

	pos9 := model.Position{Filename: "/a.go", Line: 9}

	tDerive := newTestTransformer("derive", node)
	tShape := newTestTransformer("shape", node,
		model.Annotation{Name: "plumber:template", Position: &pos9},
	)

	ts := []Transformation{
		{Node: node, Transformer: tDerive},
		{Node: node, Transformer: tShape},
	}

	result, err := filterTransformations(ts, []FileTarget{{Path: "/a.go", Line: 9}})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Transformer.GetName(), "shape")
}

func TestFilterTransformations_LineHitsImpliedByPosition(t *testing.T) {
	// Source node at line 10. The "derive" transformer has an annotation whose
	// ImpliedBy.Position is at line 7 (macro origin). Targeting line 7 should
	// match only "derive".
	node := &testNode{position: model.Position{Filename: "/a.go", Line: 10}}

	macroPos := model.Position{Filename: "/a.go", Line: 7}
	tDerive := newTestTransformer("derive", node,
		model.Annotation{
			Name:      "plumber:template",
			ImpliedBy: &model.Annotation{Position: &macroPos},
		},
	)
	tShape := newTestTransformer("shape", node)

	ts := []Transformation{
		{Node: node, Transformer: tDerive},
		{Node: node, Transformer: tShape},
	}

	result, err := filterTransformations(ts, []FileTarget{{Path: "/a.go", Line: 7}})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Transformer.GetName(), "derive")
}

func TestFilterTransformations_LineNoMatch(t *testing.T) {
	node := &testNode{position: model.Position{Filename: "/a.go", Line: 10}}
	ts := []Transformation{{Node: node, Transformer: newTestTransformer("derive", node)}}

	// Line 50 doesn't match any transformer's source or annotation position.
	_, err := filterTransformations(ts, []FileTarget{{Path: "/a.go", Line: 50}})
	assert.ErrorContains(t, err, "does not match any annotated node")
}

func TestFilterTransformations_Dedup(t *testing.T) {
	node := &testNode{position: model.Position{Filename: "/a.go", Line: 10}}
	tr := newTestTransformer("t1", node)
	ts := []Transformation{{Node: node, Transformer: tr}}

	// Two targets pointing to the same file — should not duplicate.
	result, err := filterTransformations(ts, []FileTarget{
		{Path: "/a.go"},
		{Path: "/a.go"},
	})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 1)
}

func TestFilterTransformations_NoSourceSkipped(t *testing.T) {
	// Transformer with nil Source should be skipped, not crash.
	node := &testNode{position: model.Position{Filename: "/a.go", Line: 10}}
	tr := &Shaper{BasicTransformer: BasicTransformer{
		Name:    "no-source",
		Options: model.Annotation{}, // Source is nil
	}}
	ts := []Transformation{{Node: node, Transformer: tr}}

	result, err := filterTransformations(ts, []FileTarget{{Path: "/a.go"}})
	assert.NilError(t, err)
	assert.Equal(t, len(result), 0)
}
