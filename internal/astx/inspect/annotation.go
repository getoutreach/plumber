// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements parsing of plumber annotations and struct tags from Go doc comment strings.

package inspect

import (
	"reflect"
	"strings"

	"github.com/dave/dst"
	"github.com/getoutreach/plumber/query/model"
)

// tokenizeAnnotationLine splits an annotation line into tokens, respecting
// quoted segments (", ', `). Quoted content is not split on whitespace.
// For example: `foo "bar baz" qux` → ["foo", "\"bar baz\"", "qux"]
func tokenizeAnnotationLine(line string) []string {
	var tokens []string
	i := 0
	for i < len(line) {
		// Skip whitespace.
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		if i >= len(line) {
			break
		}
		// Read one token.
		var token strings.Builder
		for i < len(line) && line[i] != ' ' && line[i] != '\t' {
			ch := line[i]
			if ch == '"' || ch == '\'' || ch == '`' {
				// Read quoted segment including the delimiters.
				quote := ch
				token.WriteByte(ch)
				i++
				for i < len(line) && line[i] != quote {
					token.WriteByte(line[i])
					i++
				}
				if i < len(line) {
					token.WriteByte(line[i]) // closing quote
					i++
				}
			} else {
				token.WriteByte(ch)
				i++
			}
		}
		if token.Len() > 0 {
			tokens = append(tokens, token.String())
		}
	}
	return tokens
}

// unquote strips matching outer quotes (", ', `) from a string if it is fully
// wrapped in them. Strings with quotes only partially wrapping (e.g. "X".value)
// are returned unchanged.
func unquote(s string) string {
	if len(s) < 2 {
		return s
	}
	first := s[0]
	if (first == '"' || first == '\'' || first == '`') && s[len(s)-1] == first {
		// Verify the closing quote is truly the end — no unmatched inner quotes
		// of the same kind. We check that removing the outer delimiters doesn't
		// leave another occurrence of the quote char which would mean the first
		// quote wasn't wrapping the entire token.
		inner := s[1 : len(s)-1]
		if !strings.ContainsRune(inner, rune(first)) {
			return inner
		}
	}
	return s
}

func ParseAnnotationsCommented(doc string) []model.Annotation {
	return parseAnnotations(doc, true, nil)
}

func ParseAnnotations(doc string, source model.Node) []model.Annotation {
	return parseAnnotations(doc, false, source)
}

// paragraphLine holds a trimmed line together with its 1-indexed position in the
// original doc string, so that parsed annotations can record their source line.
type paragraphLine struct {
	text    string
	docLine int // 1-indexed line number within the raw doc string
}

func parseAnnotations(doc string, commented bool, source model.Node) []model.Annotation {
	var annotations []model.Annotation

	// Split into paragraphs (groups of lines separated by blank lines).
	// Annotations must be in their own paragraph, separated from prose by a blank line.
	var paragraphs [][]paragraphLine
	var current []paragraphLine
	for lineIdx, line := range strings.Split(doc, "\n") {
		trimmed := strings.TrimSpace(line)
		if commented {
			trimmed = strings.TrimPrefix(trimmed, "//")
			trimmed = strings.TrimSpace(trimmed)
		}
		if trimmed == "" {
			if len(current) > 0 {
				paragraphs = append(paragraphs, current)
				current = nil
			}
		} else {
			current = append(current, paragraphLine{text: trimmed, docLine: lineIdx})
		}
	}
	if len(current) > 0 {
		paragraphs = append(paragraphs, current)
	}

	for _, para := range paragraphs {
		// Skip paragraph if any line doesn't look like an annotation.
		allAnnotations := true
		for _, pl := range para {
			tokens := tokenizeAnnotationLine(pl.text)
			if len(tokens) == 0 || !isAnnotationToken(tokens[0]) {
				allAnnotations = false
				break
			}
		}
		if !allAnnotations {
			continue
		}
		for _, pl := range para {
			tokens := tokenizeAnnotationLine(pl.text)
			ann := model.Annotation{
				Source:    source,
				Name:      tokens[0],
				NamedArgs: make(map[string]string),
				DocLine:   pl.docLine,
				Position:  computeAnnotationPosition(source, pl.docLine),
			}
			for _, token := range tokens[1:] {
				if eqIdx := strings.Index(token, "="); eqIdx > 0 {
					key := token[:eqIdx]
					value := unquote(token[eqIdx+1:])
					ann.NamedArgs[key] = value
				} else {
					ann.Args = append(ann.Args, unquote(token))
				}
			}
			annotations = append(annotations, ann)
		}
	}
	return annotations
}

func computeAnnotationPosition(source model.Node, docLine int) *model.Position {
	if source == nil {
		return nil
	}
	var order = 1
	sourcePos := source.GetPosition()

	if _, ok := source.(*model.CommentGroup); !ok {
		order = -1 // For comment groups, annotations are typically above the declaration, so we subtract docLine from the source line.
	}

	return &model.Position{
		Filename: sourcePos.Filename,
		Line:     sourcePos.Line + (docLine * order), // docLine is 1-indexed
		Column:   sourcePos.Column,
	}
}

// isAnnotationToken reports whether a token looks like a namespaced annotation
// ([@]<namespace>[.:]<name> where namespace and name are non-empty)
// or a macro reference (@<name> where name is non-empty).
func isAnnotationToken(token string) bool {
	if strings.HasPrefix(token, "@") {
		return len(token) > 1
	}
	idx := strings.IndexAny(token, ".:")
	return idx > 0 && idx < len(token)-1
}

// ParseTags parses a raw struct tag string (e.g. `json:"name,omitempty" yaml:"name"`) into
// a slice of Tag values, one per key present in the tag.
func ParseTags(raw string) []model.Tag {
	// reflect.StructTag expects the surrounding backticks to be absent.
	raw = strings.Trim(raw, "`")
	tag := reflect.StructTag(raw)

	var tags []model.Tag
	// Walk the raw string key-by-key; reflect.StructTag.Lookup gives the value.
	for raw != "" {
		// Skip leading spaces.
		for raw != "" && raw[0] == ' ' {
			raw = raw[1:]
		}
		if raw == "" {
			break
		}
		// Consume the key (up to ':').
		i := 0
		for i < len(raw) && raw[i] != ':' && raw[i] != ' ' {
			i++
		}
		if i >= len(raw) || raw[i] != ':' {
			break
		}
		key := raw[:i]
		raw = raw[i+1:]
		// Consume the quoted value.
		if raw == "" || raw[0] != '"' {
			break
		}
		j := 1
		for j < len(raw) && raw[j] != '"' {
			if raw[j] == '\\' {
				j++
			}
			j++
		}
		if j >= len(raw) {
			break
		}
		value, _ := tag.Lookup(key)
		tags = append(tags, model.Tag{Name: key, Value: value})
		raw = raw[j+1:]
	}
	return tags
}

// AnnotationsFromDecs extracts annotations from DST node decoration strings
// (comments like "// plumber:shape ..." or "// generate:once").
func AnnotationsFromDecs(decs ...dst.Decorations) model.Annotations {
	var lines []string
	for _, dec := range decs {
		for _, s := range dec {
			s = strings.TrimSpace(s)
			if strings.HasPrefix(s, "//") {
				line := strings.TrimPrefix(s, "//")
				line = strings.TrimSpace(line)
				lines = append(lines, line)
			}
		}
	}
	if len(lines) == 0 {
		return nil
	}
	return ParseAnnotations(strings.Join(lines, "\n"), nil)
}
