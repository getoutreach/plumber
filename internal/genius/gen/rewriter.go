// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides filename rewriting utilities for remapping output paths during code generation.

package gen

import (
	"path"
	"strings"
)

func FullPathRewrite(oldPath, newPath string) FilenameRewriter {
	return func(filename string) (bool, string) {
		if filename == oldPath {
			return true, newPath
		}
		return false, filename
	}
}

func PrefixRewrite(prefix, newPrefix string, rewriters ...FilenameRewriter) FilenameRewriter {
	return func(filename string) (bool, string) {
		if strings.HasPrefix(filename, prefix) {
			filename = newPrefix + strings.TrimPrefix(filename, prefix)
			return true, rewriteFilename(filename, rewriters...)
		}
		return false, filename
	}
}

func FileNameSuffix(suffix string, rewriters ...FilenameRewriter) FilenameRewriter {
	return func(filename string) (bool, string) {
		base := path.Base(filename)
		dir := path.Dir(filename)
		ext := path.Ext(filename)
		withoutExtension := strings.TrimSuffix(base, ext)
		if !strings.HasSuffix(withoutExtension, suffix) {
			filename = path.Join(dir, withoutExtension+suffix+ext)
			return true, rewriteFilename(filename, rewriters...)
		}
		return false, filename
	}
}
