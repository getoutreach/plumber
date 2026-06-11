// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file contains tests for build-constraint extraction from
// Go source content.

package astx

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestPositiveBuildTags_NoDirective(t *testing.T) {
	src := []byte(`// Copyright 2026 Outreach.

package foo

func Bar() {}
`)
	tags, err := PositiveBuildTags(src)
	assert.NilError(t, err)
	assert.Assert(t, tags == nil)
}

func TestPositiveBuildTags_SingleTag(t *testing.T) {
	src := []byte(`//go:build goverter

package foo
`)
	tags, err := PositiveBuildTags(src)
	assert.NilError(t, err)
	assert.DeepEqual(t, tags, []string{"goverter"})
}

func TestPositiveBuildTags_AndExpression(t *testing.T) {
	src := []byte(`//go:build goverter && other

package foo
`)
	tags, err := PositiveBuildTags(src)
	assert.NilError(t, err)
	assert.DeepEqual(t, tags, []string{"goverter", "other"})
}

func TestPositiveBuildTags_OrExpression(t *testing.T) {
	src := []byte(`//go:build goverter || other

package foo
`)
	tags, err := PositiveBuildTags(src)
	assert.NilError(t, err)
	assert.DeepEqual(t, tags, []string{"goverter", "other"})
}

func TestPositiveBuildTags_PurelyNegative(t *testing.T) {
	src := []byte(`//go:build !goverter

package foo
`)
	tags, err := PositiveBuildTags(src)
	assert.NilError(t, err)
	assert.Assert(t, tags == nil, "expected nil, got %v", tags)
}

func TestPositiveBuildTags_MixedNegativePositive(t *testing.T) {
	src := []byte(`//go:build !goverter && other

package foo
`)
	tags, err := PositiveBuildTags(src)
	assert.NilError(t, err)
	assert.DeepEqual(t, tags, []string{"other"})
}

func TestPositiveBuildTags_DoubleNegationKeepsTag(t *testing.T) {
	src := []byte(`//go:build !(!goverter)

package foo
`)
	tags, err := PositiveBuildTags(src)
	assert.NilError(t, err)
	assert.DeepEqual(t, tags, []string{"goverter"})
}

func TestPositiveBuildTags_DeduplicatedAndSorted(t *testing.T) {
	src := []byte(`//go:build other && goverter && goverter

package foo
`)
	tags, err := PositiveBuildTags(src)
	assert.NilError(t, err)
	assert.DeepEqual(t, tags, []string{"goverter", "other"})
}

func TestPositiveBuildTags_PrecededByCommentsAndBlankLines(t *testing.T) {
	src := []byte(`// Copyright 2026 Outreach.

// Description: something.

//go:build goverter

package foo
`)
	tags, err := PositiveBuildTags(src)
	assert.NilError(t, err)
	assert.DeepEqual(t, tags, []string{"goverter"})
}

func TestPositiveBuildTags_StopsAtPackage(t *testing.T) {
	// Directives after the package declaration are not real build
	// directives — they must be ignored.
	src := []byte(`package foo

//go:build goverter
`)
	tags, err := PositiveBuildTags(src)
	assert.NilError(t, err)
	assert.Assert(t, tags == nil)
}

func TestPositiveBuildTags_LegacyPlusBuildIgnored(t *testing.T) {
	// We only honor //go:build per the spec; // +build legacy lines
	// should be skipped without errors.
	src := []byte(`// +build goverter

package foo
`)
	tags, err := PositiveBuildTags(src)
	assert.NilError(t, err)
	assert.Assert(t, tags == nil)
}

func TestPositiveBuildTags_MalformedReturnsError(t *testing.T) {
	src := []byte(`//go:build goverter &&

package foo
`)
	tags, err := PositiveBuildTags(src)
	assert.Assert(t, err != nil, "expected error for malformed //go:build")
	assert.Assert(t, tags == nil)
}
