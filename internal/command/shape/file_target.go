// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements file-path-based filtering of transformations, allowing the shape
// command to restrict execution to specific files and line numbers.

package shape

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/query/model"
)

// FileTarget represents a file path with an optional line number for filtering transformations.
// When Line is 0, all transformations in the file are included.
type FileTarget struct {
	Path string // absolute file path
	Line int    // 0 means no line filter — include all transformations in the file
}

// ParseFileTargets parses CLI arguments in the format "path[:line]" into FileTarget values.
// Paths are resolved to absolute paths for matching against model.Position.Filename.
// Returns an error for malformed line numbers or non-positive line values.
func ParseFileTargets(args []string) ([]FileTarget, error) {
	var targets []FileTarget
	for _, arg := range args {
		target, err := parseFileTarget(arg)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, nil
}

// parseFileTarget parses a single "path[:line]" argument into a FileTarget.
func parseFileTarget(arg string) (FileTarget, error) {
	var path string
	var line int

	// Split on the last colon to separate path from optional line number.
	// We use the last colon to avoid issues with Windows drive letters (e.g. C:\foo).
	if idx := strings.LastIndex(arg, ":"); idx > 0 {
		suffix := arg[idx+1:]
		// Only treat it as a line number if the suffix is numeric.
		if n, err := strconv.Atoi(suffix); err == nil {
			if n <= 0 {
				return FileTarget{}, fmt.Errorf("line number must be positive, got %d in %q", n, arg)
			}
			path = arg[:idx]
			line = n
		} else {
			// Not a number — treat the entire arg as a path.
			path = arg
		}
	} else {
		path = arg
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return FileTarget{}, fmt.Errorf("failed to resolve path %q: %w", path, err)
	}

	return FileTarget{Path: absPath, Line: line}, nil
}

// filterTransformations filters the collected transformations to only those matching
// the provided file targets. When targets is empty, all transformations are returned
// unchanged.
//
// Matching rules:
//   - File-only target (Line == 0): include all transformations whose node is in that file.
//   - File+line target: find the node whose doc-comment-to-declaration range covers the
//     line. If the line hits a transformer annotation (plumber:derive/shape/render), include
//     only that specific transformation. If it hits a modifier annotation, include the
//     nearest transformer annotation above. If it hits the declaration itself or a
//     non-annotation doc line, include all transformations for that node.
//   - If a file+line target matches no annotated node, an error is returned.
func filterTransformations(transformations []Transformation, targets []FileTarget) ([]Transformation, error) {
	if len(targets) == 0 {
		return transformations, nil
	}

	var result []Transformation
	for _, target := range targets {
		matched, err := matchTarget(transformations, target)
		if err != nil {
			return nil, err
		}
		result = append(result, matched...)
	}

	return dedupTransformations(result), nil
}

// matchTarget finds all transformations matching a single FileTarget.
func matchTarget(transformations []Transformation, target FileTarget) ([]Transformation, error) {
	// File-only target: include all transformations in this file.
	var result []Transformation
	for _, t := range transformations {
		if t.Transformer.GetOptions().Source != nil {
			p := t.Transformer.GetOptions().Source.GetPosition()
			if p.Filename == target.Path {
				result = append(result, t)
			}
		}
	}

	if target.Line == 0 {
		return result, nil
	}

	var filtered []Transformation

	for _, t := range result {
		// Transformer source position is the declaration line, so it matches
		// if the target line hits the declaration itself.
		p := t.Transformer.GetOptions().Source.GetPosition()
		if p.Line == target.Line {
			filtered = append(filtered, t)
			continue
		}

		for _, ann := range t.Transformer.GetAnnotations() {
			// Position is matching position of the macro
			if ann.ImpliedBy != nil && ann.ImpliedBy.Position != nil {
				p = *ann.ImpliedBy.Position
				if p.Line == target.Line {
					filtered = append(filtered, t)
					break
				}
			}
			// Position is matching position of the annotation itself
			// all subsequent annotations has the position derived from the same position + doc line
			if ann.Position != nil {
				p = *ann.Position
				if p.Line == target.Line {
					filtered = append(filtered, t)
					break
				}
			}
		}
	}

	if len(filtered) > 0 {
		return filtered, nil
	}
	return nil, fmt.Errorf("line %d in %s does not match any annotated node", target.Line, target.Path)
}

// nodeLineRange computes the approximate line range [start, end] for a node,
// where start is the first line of the doc comment and end is the declaration line.
// The doc comment lines end right before the declaration (Go doc comment convention).
func nodeLineRange(node model.Node) (start, end int) {
	pos := node.GetPosition()
	end = pos.Line
	doc := node.GetDoc()
	if doc == "" {
		return end, end
	}
	docLines := strings.Count(doc, "\n")
	// Doc text from go/ast ends with a trailing newline, so the actual number of
	// content lines equals the newline count (not +1).
	if docLines == 0 {
		docLines = 1
	}
	start = end - docLines
	return start, end
}

// findAnnotationByAbsLine finds the annotation whose absolute source line matches
// the target line. The absolute line is computed as: nodeStart + annotation.DocLine - 1.
// Returns the annotation index, or -1 if no match.
func findAnnotationByAbsLine(annotations model.Annotations, targetLine, nodeStart int) int {
	for i, ann := range annotations {
		if ann.DocLine == 0 {
			continue // no line tracking for this annotation
		}
		absLine := nodeStart + ann.DocLine - 1
		if absLine == targetLine {
			return i
		}
	}
	return -1
}

// isTransformerAnnotation reports whether the annotation name is an entry-point
// transformer annotation that creates a new Transformation.
func isTransformerAnnotation(name string) bool {
	return name == contract.TransformationDerive ||
		name == contract.TransformationShape ||
		name == contract.TransformationRender
}

// countTransformerAnnotationsUpTo counts how many transformer annotations appear
// at or before index idx (0-indexed). This gives the 0-indexed transformer/transformation
// position within the node's transformations slice.
func countTransformerAnnotationsUpTo(annotations model.Annotations, idx int) int {
	count := 0
	for i := 0; i <= idx; i++ {
		if isTransformerAnnotation(annotations[i].Name) {
			count++
		}
	}
	return count - 1 // 0-indexed
}

// findNearestTransformerAbove walks backward from idx through annotations to find
// the nearest transformer annotation above. Returns the 0-indexed transformer position
// within the transformations slice, or -1 if none found.
func findNearestTransformerAbove(annotations model.Annotations, idx int) int {
	for i := idx - 1; i >= 0; i-- {
		if isTransformerAnnotation(annotations[i].Name) {
			return countTransformerAnnotationsUpTo(annotations, i)
		}
	}
	return -1
}

// dedupTransformations removes duplicate transformations from the slice,
// preserving order. Two transformations are considered duplicates when they
// share the same Node and Transformer pointer.
func dedupTransformations(ts []Transformation) []Transformation {
	type key struct {
		node        model.Node
		transformer Transformer
	}
	seen := map[key]struct{}{}
	var result []Transformation
	for _, t := range ts {
		k := key{node: t.Node, transformer: t.Transformer}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		result = append(result, t)
	}
	return result
}
