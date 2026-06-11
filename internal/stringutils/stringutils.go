// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides miscellaneous string utility functions including random string generation.

// Package stringutils provides miscellaneous string utility functions for use across the plumber codebase.
package stringutils

import (
	"crypto/rand"
	"math/big"
)

// letterRunes contains the set of characters used for generating random strings.
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		a, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letterRunes)))) //nolint:errcheck //Why: int can't return error
		b[i] = letterRunes[a.Int64()]
	}
	return string(b)
}
