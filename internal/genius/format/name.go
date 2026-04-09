// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides name formatting utilities including PascalCase, CamelCase, SnakeCase, and Go struct name normalization with common initialisms.

// Package format provides type and value formatting utilities including case conversion, name normalization, and type serialization helpers.
package format

import (
	"regexp"
	"strings"

	"github.com/gobeam/stringy"
)

var (
	reNumbers = regexp.MustCompile(`(_\d+)`)
)

// PascalCase convert string to PascalCased form
func PascalCase(s string) string {
	return stringy.New(stringy.New(s).CamelCase().Get()).UcFirst()
}

// PascalCase convert string to PascalCased form
func ProtoPascalCase(s string) string {
	indexes := reNumbers.FindAllStringIndex(s, -1)
	laststart := 0
	result := []string{}
	for _, element := range indexes {
		result = append(result, PascalCase(s[laststart:element[0]]), s[element[0]:element[1]])
		laststart = element[1]
	}
	result = append(result, PascalCase(s[laststart:]))
	return strings.Join(result, "")
}

// CamelCase convert string to camelCased form
func CamelCase(s string) string {
	return stringy.New(stringy.New(s).CamelCase().Get()).LcFirst()
}

// SnakeCase convert "SnAke cased" string to SnAke_cased form
func SnakeCase(s string) string {
	return stringy.New(s).SnakeCase().Get()
}

// SnakeLowerCase convert "SnAke cased" string to snake_cased form
func SnakeLowerCase(s string) string {
	return stringy.New(s).SnakeCase().ToLower()
}

// NormalizeFieldmask converts any string slice to slice of snake cased strings
func NormalizeFieldmask(fieldmask []string) []string {
	normalized := make([]string, len(fieldmask))
	for i, s := range fieldmask {
		normalized[i] = SnakeCase(s)
	}
	return normalized
}

func ToProtoName(name string) string {
	return GoCamelCase(name)
}

// commonInitialisms is a set of common initialisms.
// Only add entries that are highly unlikely to be non-initialisms.
// For instance, "ID" is fine (Freudian code is rare), but "AND" is not.
// taken from https://github.com/golang/lint/blob/master/lint.go#L740
var commonInitialisms = map[string]bool{
	"ACL":   true,
	"API":   true,
	"ASCII": true,
	"CPU":   true,
	"CSS":   true,
	"DNS":   true,
	"EOF":   true,
	"GUID":  true,
	"HTML":  true,
	"HTTP":  true,
	"HTTPS": true,
	"ID":    true,
	"IP":    true,
	"JSON":  true,
	"LHS":   true,
	"QPS":   true,
	"RAM":   true,
	"RHS":   true,
	"RPC":   true,
	"SLA":   true,
	"SMTP":  true,
	"SQL":   true,
	"SSH":   true,
	"TCP":   true,
	"TLS":   true,
	"TTL":   true,
	"UDP":   true,
	// "UI":    true,
	"UID":  true,
	"UUID": true,
	"URI":  true,
	"URL":  true,
	"UTF8": true,
	"VM":   true,
	"XML":  true,
	"XMPP": true,
	"XSRF": true,
	"XSS":  true,
}

type initialism struct {
	Re   *regexp.Regexp
	Name string
}

func buildInitialisms(in map[string]bool) []initialism {
	res := []initialism{}
	for name := range in {
		res = append(res, initialism{
			Name: name,
			Re:   regexp.MustCompile(PascalCase(strings.ToLower(name))),
		})
	}
	return res
}

var commonInitialismStructs = buildInitialisms(commonInitialisms)

// ToStructName converts string using Go convention for abbreviations
// This should be somewhat automated. Hasn't found yet the struct
func ToStructName(name string) string {
	name = PascalCase(name)
	for _, r := range commonInitialismStructs {
		name = r.Re.ReplaceAllString(name, r.Name)
	}
	return name
}
