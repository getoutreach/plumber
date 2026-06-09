// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements helpers for extracting build constraint
// information from Go source files so callers (such as the shape command's
// output restoration step) can group files by the build tags required to
// parse them.

package astx

import (
	"bufio"
	"bytes"
	"go/build/constraint"
	"sort"
	"strings"

	"github.com/samber/lo"
)

// PositiveBuildTags scans content for a //go:build directive and returns the
// sorted, de-duplicated set of positive tag names appearing in the expression.
// Negated tags (anywhere inside a *constraint.NotExpr subtree) are ignored.
//
// Returns (nil, nil) when no //go:build directive is present in the header of
// the file. Returns (nil, err) when a //go:build directive is found but cannot
// be parsed by go/build/constraint; callers may log the error and treat the
// file as having no constraints.
//
// Scanning stops at the first line that looks like a package declaration so
// that build directives appearing after `package` (which Go ignores) are
// likewise ignored here.
func PositiveBuildTags(content []byte) ([]string, error) {
	expr, err := findBuildExpr(content)
	if err != nil {
		return nil, err
	}
	if expr == nil {
		return nil, nil
	}
	var tags []string
	collectPositiveTags(expr, false, &tags)
	if len(tags) == 0 {
		return nil, nil
	}
	tags = lo.Uniq(tags)
	sort.Strings(tags)
	return tags, nil
}

// findBuildExpr scans the header of content for the first //go:build line and
// returns the parsed expression. Returns (nil, nil) when no directive is
// found. Returns (nil, err) when a directive is present but unparsable.
func findBuildExpr(content []byte) (constraint.Expr, error) {
	scanner := bufio.NewScanner(bytes.NewReader(content))
	// Generated headers can be longer than the default 64KiB scanner buffer
	// when imports/documentation grow; raise the limit defensively.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// Once we hit the package clause, the //go:build directive (if any)
		// must have appeared earlier — stop scanning to avoid picking up
		// directives in body comments.
		if strings.HasPrefix(line, "package ") || line == "package" {
			return nil, nil
		}
		if constraint.IsGoBuild(line) {
			return constraint.Parse(line)
		}
		// Anything else (other line comments, +build legacy lines, block
		// comments) is skipped so we can keep looking for //go:build.
	}
	return nil, nil
}

// collectPositiveTags walks expr, appending each *TagExpr's tag to out unless
// it appears under a *NotExpr (in which case it is treated as a negative
// constraint and ignored). The negated flag flips at each *NotExpr.
func collectPositiveTags(expr constraint.Expr, negated bool, out *[]string) {
	switch e := expr.(type) {
	case *constraint.TagExpr:
		if !negated {
			*out = append(*out, e.Tag)
		}
	case *constraint.NotExpr:
		collectPositiveTags(e.X, !negated, out)
	case *constraint.AndExpr:
		collectPositiveTags(e.X, negated, out)
		collectPositiveTags(e.Y, negated, out)
	case *constraint.OrExpr:
		collectPositiveTags(e.X, negated, out)
		collectPositiveTags(e.Y, negated, out)
	}
}
