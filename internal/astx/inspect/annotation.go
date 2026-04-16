// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements parsing of plumber annotations and struct tags from Go doc comment strings.

package inspect

import (
	"reflect"
	"strings"

	"github.com/getoutreach/plumber/query/model"
)

func ParseAnnotations(doc string) []model.Annotation {
	var annotations []model.Annotation

	// Split into paragraphs (groups of lines separated by blank lines).
	// Annotations must be in their own paragraph, separated from prose by a blank line.
	var paragraphs [][]string
	var current []string
	for _, line := range strings.Split(doc, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			if len(current) > 0 {
				paragraphs = append(paragraphs, current)
				current = nil
			}
		} else {
			current = append(current, trimmed)
		}
	}
	if len(current) > 0 {
		paragraphs = append(paragraphs, current)
	}

	for _, para := range paragraphs {
		// Skip paragraph if any line doesn't look like an annotation.
		allAnnotations := true
		for _, line := range para {
			if !isAnnotationToken(strings.Fields(line)[0]) {
				allAnnotations = false
				break
			}
		}
		if !allAnnotations {
			continue
		}
		for _, line := range para {
			tokens := strings.Fields(line)
			ann := model.Annotation{
				Name:      tokens[0],
				NamedArgs: make(map[string]string),
			}
			for _, token := range tokens[1:] {
				if strings.Contains(token, "=") {
					parts := strings.SplitN(token, "=", 2)
					ann.NamedArgs[parts[0]] = parts[1]
				} else {
					ann.Args = append(ann.Args, token)
				}
			}
			annotations = append(annotations, ann)
		}
	}
	return annotations
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
		for len(raw) > 0 && raw[0] == ' ' {
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
		if len(raw) == 0 || raw[0] != '"' {
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
