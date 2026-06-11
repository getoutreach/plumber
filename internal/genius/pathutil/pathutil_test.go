package pathutil

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestExtendFilename(t *testing.T) {
	assert.Equal(t, "dirname/file_ref.proto", ExtendFilename("dirname/file.proto", "_ref"))
}

func TestFileDir(t *testing.T) {
	assert.Equal(t, "dirname/ref/file.proto", ExtendFileDir("dirname/file.proto", "ref"))
}
