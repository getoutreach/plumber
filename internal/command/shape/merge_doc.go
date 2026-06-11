// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements doc-comment merging helpers for inplace merge.
// The merge rule is line-level: each comment line emitted by the generator is
// merged into the existing decoration list, never duplicated, and inserted just
// after the last common "anchor" line shared by both sides. When no anchor has
// been seen yet, new generator lines are appended at the end. This guarantees
// manually-added lines are never lost while still allowing the generator to
// add or seed lines around them.
//
// Equality between two comment lines compares the text after stripping the
// leading // (or surrounding /* */) and trimming whitespace. Pure newline
// entries in the decoration list are filtered out before merging and a fresh
// "\n" separator is injected between every kept item on rebuild. Inline
// trailing comments (Decs.End) are never touched by these helpers.

package shape

import (
	"strings"

	"github.com/dave/dst"
)

// hasDocComment reports whether the given decoration list contains any line
// that looks like a Go comment (// ... or /* ... */). Pure newline ("\n")
// entries do not count.
func hasDocComment(decs dst.Decorations) bool {
	for _, s := range decs {
		t := strings.TrimSpace(s)
		if strings.HasPrefix(t, "//") || strings.HasPrefix(t, "/*") {
			return true
		}
	}
	return false
}

// commentItems returns the comment-only entries from a decoration list,
// preserving their original raw form. Pure newline separators and any other
// non-comment artifact are dropped. The returned slice is independent of the
// input.
func commentItems(decs dst.Decorations) []string {
	out := make([]string, 0, len(decs))
	for _, s := range decs {
		t := strings.TrimSpace(s)
		if strings.HasPrefix(t, "//") || strings.HasPrefix(t, "/*") {
			out = append(out, s)
		}
	}
	return out
}

// normalizeCommentLine returns the comparable text of a comment item: the
// payload after stripping the leading // (or surrounding /* */ wrapper) and
// trimming whitespace. Empty paragraph-break lines (`//`) normalize to "".
// Non-comment strings normalize to themselves (trimmed) so unexpected entries
// do not accidentally collapse together.
func normalizeCommentLine(s string) string {
	t := strings.TrimSpace(s)
	switch {
	case strings.HasPrefix(t, "//"):
		return strings.TrimSpace(strings.TrimPrefix(t, "//"))
	case strings.HasPrefix(t, "/*") && strings.HasSuffix(t, "*/"):
		inner := t[2 : len(t)-2]
		return strings.TrimSpace(inner)
	default:
		return t
	}
}

// rebuildDecorations turns a flat list of comment items back into a
// dst.Decorations slice using dst's canonical format: items are stored as a
// plain sequence with no "\n" separators between them and no trailing
// newline. dst's renderer takes care of placing each comment on its own
// line. Paragraph breaks within a comment block are represented as a literal
// "//" element in items (the same form produced by dst's parser).
func rebuildDecorations(items []string) dst.Decorations {
	if len(items) == 0 {
		return dst.Decorations{}
	}
	out := make(dst.Decorations, len(items))
	copy(out, items)
	return out
}

// mergeDocComment merges the generated doc comment into the existing one
// using a line-level union: each generated comment line is added to the
// existing list (after the last anchor shared by both sides) unless it is
// already present. Manual lines are never removed; existing formatting is
// preferred for shared lines. The function reports whether existing was
// modified.
//
// Algorithm sketch:
//
//	existingItems  = comment items extracted from *existing
//	generatedItems = comment items extracted from generated
//	result = copy(existingItems); lastAnchor = -1
//	for gen in generatedItems:
//	    found = first i > lastAnchor in result with normalize(result[i]) == normalize(gen)
//	    if found != -1:
//	        lastAnchor = found            // shared line; keep existing's raw form
//	    elif lastAnchor == -1:
//	        append gen; lastAnchor = len(result)-1
//	    else:
//	        insert gen at lastAnchor+1; lastAnchor++
//	if result equals existingItems (by raw form): no-op
//	else: *existing = rebuild(result)
func mergeDocComment(existing *dst.Decorations, generated dst.Decorations) bool {
	if existing == nil {
		return false
	}

	existingItems := commentItems(*existing)
	generatedItems := commentItems(generated)
	if len(generatedItems) == 0 {
		return false
	}

	result := make([]string, len(existingItems))
	copy(result, existingItems)
	lastAnchor := -1

	for _, gen := range generatedItems {
		genKey := normalizeCommentLine(gen)
		found := -1
		for i := lastAnchor + 1; i < len(result); i++ {
			if normalizeCommentLine(result[i]) == genKey {
				found = i
				break
			}
		}
		switch {
		case found != -1:
			lastAnchor = found
		case lastAnchor == -1:
			result = append(result, gen)
			lastAnchor = len(result) - 1
		default:
			// insert gen at lastAnchor+1
			result = append(result, "")
			copy(result[lastAnchor+2:], result[lastAnchor+1:])
			result[lastAnchor+1] = gen
			lastAnchor++
		}
	}

	if sameItems(existingItems, result) {
		return false
	}

	*existing = rebuildDecorations(result)
	return true
}

// sameItems reports whether two raw comment-item slices are identical.
func sameItems(a, b []string) bool {
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

// findContainingGenDecl returns the *dst.GenDecl in file that contains the
// given TypeSpec. The doc comment for `type Foo struct {...}` lives on the
// containing GenDecl rather than on the TypeSpec itself, so this lookup is
// necessary when merging type-level docs.
func findContainingGenDecl(file *dst.File, ts *dst.TypeSpec) *dst.GenDecl {
	if file == nil || ts == nil {
		return nil
	}
	for _, decl := range file.Decls {
		gd, ok := decl.(*dst.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gd.Specs {
			if spec == dst.Node(ts) {
				return gd
			}
		}
	}
	return nil
}

// findFieldByName scans a FieldList for an entry whose declared names (or
// derived name for embedded fields) match the given name. It is used to
// locate the existing struct field or interface method that a generated
// entry collides with so that field-level doc merging can be applied.
func findFieldByName(list *dst.FieldList, name string) *dst.Field {
	if list == nil || name == "" {
		return nil
	}
	for _, f := range list.List {
		for _, n := range fieldNames(f) {
			if n == name {
				return f
			}
		}
	}
	return nil
}

// findFieldByEmbedKey scans a FieldList for an entry that is an embedded
// interface (Names is empty) and whose type expression matches the given
// embed key (as produced by embedKey). Used by interface merge to locate
// the existing embed corresponding to a generated one.
func findFieldByEmbedKey(list *dst.FieldList, key string) *dst.Field {
	if list == nil || key == "" {
		return nil
	}
	for _, f := range list.List {
		if len(f.Names) > 0 {
			continue
		}
		if embedKey(f.Type) == key {
			return f
		}
	}
	return nil
}
