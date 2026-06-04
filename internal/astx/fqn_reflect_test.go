// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: Unit tests for FQNFromReflectType conversions.

package astx

import (
	"net/url"
	"reflect"
	"testing"
)

type sampleStruct struct{ X int }

func TestFQNFromReflectType(t *testing.T) {
	var (
		chOutInt <-chan int
		chInInt  chan<- int
	)
	cases := []struct {
		name string
		t    reflect.Type
		want string
	}{
		{"string", reflect.TypeOf(""), "string"},
		{"int", reflect.TypeOf(0), "int"},
		{"bool", reflect.TypeOf(true), "bool"},
		{"error", reflect.TypeOf((*error)(nil)).Elem(), "error"},
		{"named", reflect.TypeOf(sampleStruct{}), `"github.com/getoutreach/plumber/internal/astx".sampleStruct`},
		{"pointer-named", reflect.TypeOf(&url.URL{}), `*"net/url".URL`},
		{"slice-string", reflect.TypeOf([]string{}), "[]string"},
		{"map-string-int", reflect.TypeOf(map[string]int{}), "map[string]int"},
		{"slice-named", reflect.TypeOf([]sampleStruct(nil)), `[]"github.com/getoutreach/plumber/internal/astx".sampleStruct`},
		{"chan-int", reflect.TypeOf(make(chan int)), "chan int"},
		{"recv-chan-int", reflect.TypeOf(chOutInt), "<-chan int"},
		{"send-chan-int", reflect.TypeOf(chInInt), "chan<- int"},
		{"array", reflect.TypeOf([3]int{}), "[3]int"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := FQNFromReflectType(c.t).String()
			if got != c.want {
				t.Errorf("FQNFromReflectType(%v) = %q, want %q", c.t, got, c.want)
			}
		})
	}
}

func TestFQNFromReflectTypeNil(t *testing.T) {
	got := FQNFromReflectType(nil).String()
	if got != "nil" {
		t.Errorf("expected 'nil' for nil reflect.Type, got %q", got)
	}
}
