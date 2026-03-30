package pathutil

import (
	"os"
	"path"
	"strings"
)

func ExtendFilename(filename, suffix string) string {
	dir := path.Dir(filename)
	base := path.Base(filename)

	ext := path.Ext(base)

	return path.Join(dir, strings.TrimSuffix(base, ext)+suffix+ext)
}

func ExtendFileDir(filename, suffix string) string {
	dir := path.Dir(filename)
	base := path.Base(filename)
	return path.Join(dir, suffix, base)
}

// Exists returns whether the given path exists
func Exists(p string) (bool, error) {
	_, err := os.Stat(p)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		err = nil
	}
	return false, err
}

// MustExists returns whether the given path exists
func MustExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
