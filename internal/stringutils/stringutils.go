// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides miscellaneous string utility functions including random string generation.

// Package stringutils provides miscellaneous string utility functions for use across the plumber codebase.
package stringutils

import (
	"math/rand"
	"time"
)

func init() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
