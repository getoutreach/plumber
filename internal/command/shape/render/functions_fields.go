// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the fields_match template function, which selects the
// fields of one field set that have a counterpart (by name) in another field set using either
// a strict or a tolerant matching method.

package render

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/getoutreach/plumber/query/model"
)

const (
	// FieldsMatchStrict matches fields by exact, case-sensitive name equality.
	FieldsMatchStrict = "strict"
	// FieldsMatchTolerant matches fields by normalized names and struct tag names.
	FieldsMatchTolerant = "tolerant"
)

// fieldsMatch returns every field of a that has a counterpart in b, preserving the order of a.
//
// Supported methods:
//   - "strict" (default when method is empty): names must be identical (`UserID` == `UserID`).
//   - "tolerant": each field is described by a set of keys — its normalized name plus the
//     normalized name part of every struct tag (`json:"user_id,omitempty"` → `userid`; tags
//     with value "-" or an empty name are ignored). Normalization lowercases and drops every
//     non-alphanumeric rune, so `UserID`, `userId`, `user_id` and `USER-ID` all match. Two
//     fields match when their key sets intersect.
//
// Nil entries in either slice are ignored. An unknown method returns an error.
func fieldsMatch(a, b []*model.Var, method string) ([]*model.Var, error) {
	var keys func(*model.Var) []string
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "", FieldsMatchStrict:
		keys = strictFieldKeys
	case FieldsMatchTolerant:
		keys = tolerantFieldKeys
	default:
		return nil, fmt.Errorf("fields_match: unknown method %q (expected %q or %q)",
			method, FieldsMatchStrict, FieldsMatchTolerant)
	}

	index := make(map[string]struct{})
	for _, v := range b {
		if v == nil {
			continue
		}
		for _, k := range keys(v) {
			index[k] = struct{}{}
		}
	}

	matched := make([]*model.Var, 0, len(a))
	for _, v := range a {
		if v == nil {
			continue
		}
		for _, k := range keys(v) {
			if _, ok := index[k]; ok {
				matched = append(matched, v)
				break
			}
		}
	}
	return matched, nil
}

// strictFieldKeys returns the field name as its only key.
func strictFieldKeys(v *model.Var) []string {
	if v.Name == "" {
		return nil
	}
	return []string{v.Name}
}

// tolerantFieldKeys returns the normalized field name and normalized struct tag names.
func tolerantFieldKeys(v *model.Var) []string {
	var out []string
	if n := normalizeFieldName(v.Name); n != "" {
		out = append(out, n)
	}
	for _, tag := range v.Tags {
		name, _, _ := strings.Cut(tag.Value, ",")
		if name == "-" {
			continue
		}
		if n := normalizeFieldName(name); n != "" {
			out = append(out, n)
		}
	}
	return out
}

// normalizeFieldName lowercases s and drops every rune that is not a letter or digit.
func normalizeFieldName(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}
