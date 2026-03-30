package gen

import (
	"fmt"
	"io"
	"testing"

	"gotest.tools/v3/assert"
)

func TestBlockWriter(t *testing.T) {
	t.Skip("Blocks not supported")
	op := NewBufferFileOpenner()

	op.Write("test.go", []byte(`
// header
// <<Stencil::Block(header)>>
header content
// <</Stencil::Block>>

///Block-API(content)
before
// <<plumber::Block(inner)>>
my inner
// <</plumber::Block>>
after
/// EndBlock-API(content)

// <<Stencil::Block(footer)>>
footer content
// <</Stencil::Block>>
`))

	bw := NewBlockWriterWithOpenner("test.go", op)

	bw.Write([]byte(`
// <<plumber::Block(inner)>>
// <</plumber::Block>>
CONTENT`))
	assert.NilError(t, bw.Close())

	f, _ := op.Open("test.go")

	b, _ := io.ReadAll(f)

	expected := `
// header
// <<Stencil::Block(header)>>
header content
// <</Stencil::Block>>

///Block-API(content)

// <<plumber::Block(inner)>>
my inner
// <</plumber::Block>>
CONTENT
///EndBlock-API(content)

// <<Stencil::Block(footer)>>
footer content
// <</Stencil::Block>>
`

	assert.Equal(t, string(b), expected)
}

func TestWriterWithEmptyBlocks(t *testing.T) {
	op := NewBufferFileOpenner()

	op.Write("test.go", []byte(`
	import (
		// <<plumber::Block(customImports)>>
        1
		// <</plumber::Block>>
	)

	// Base is the base for all services.
	type Base struct {
		store store.Store
		// <<plumber::Block(customDependencies)>>
        2 $filter
		// <</plumber::Block>>
	}

    func Test() {
        // <<plumber::Fragment(pestFunction locked)>>
        // <</plumber::Fragment>>
    }
`))

	bw := NewBlockWriterWithOpenner("test.go", op)

	bw.Write([]byte(`
	import (
		// <<plumber::Block(customImports)>>
		// <</plumber::Block>>
	)

	// Base is the base for all services.
	type Base struct {
		store store.Store
		// <<plumber::Block(customDependencies)>>
		// <</plumber::Block>>
	}`))
	assert.NilError(t, bw.Close())

	f, _ := op.Open("test.go")

	b, _ := io.ReadAll(f)

	expected := `
	import (
		// <<plumber::Block(customImports)>>
        1
		// <</plumber::Block>>
	)

	// Base is the base for all services.
	type Base struct {
		store store.Store
		// <<plumber::Block(customDependencies)>>
        2 $filter
		// <</plumber::Block>>
	}`

	assert.Equal(t, string(b), expected)
}

func TestWriterHashes(t *testing.T) {
	op := NewBufferFileOpenner()

	op.Write("test.go", []byte(`
	import (
		# <<plumber::Block(customImports)>>
        1
		# <</plumber::Block>>
	)

	// Base is the base for all services.
	type Base struct {
		store store.Store
		# <<plumber::Block(customDependencies)>>
        2
		# <</plumber::Block>>
	}

    func Test() {
        # <<plumber::Fragment(pestFunction locked)>>
        # <</plumber::Fragment>>
    }
`))

	bw := NewBlockWriterWithOpenner("test.go", op)

	bw.Write([]byte(`
	import (
		# <<plumber::Block(customImports)>>
		# <</plumber::Block>>
	)

	// Base is the base for all services.
	type Base struct {
		store store.Store
		# <<plumber::Block(customDependencies)>>
		# <</plumber::Block>>
	}`))
	assert.NilError(t, bw.Close())

	f, _ := op.Open("test.go")

	b, _ := io.ReadAll(f)

	expected := `
	import (
		# <<plumber::Block(customImports)>>
        1
		# <</plumber::Block>>
	)

	// Base is the base for all services.
	type Base struct {
		store store.Store
		# <<plumber::Block(customDependencies)>>
        2
		# <</plumber::Block>>
	}`

	assert.Equal(t, string(b), expected)
}

func TestWriterFragment(t *testing.T) {
	op := NewBufferFileOpenner()

	op.Write("test.go", []byte(`

    import (
        // <<plumber::Fragment(import)>>
		// <<plumber::Block(customImports)>>
        1
		// <</plumber::Block>>
        // <</plumber::Fragment>>
	)


    func Test() {
        // <<plumber::Fragment(pestFunction locked)>>
        locked
        // <</plumber::Fragment>>
    }
`))

	bw := NewBlockWriterWithOpenner("test.go", op)

	bw.Write([]byte(`
	import (
        // <<plumber::Fragment(import)>>
		// <<plumber::Block(customImports)>>
        // default
		// <</plumber::Block>>
        // <</plumber::Fragment>>
	)


    func Test2() {
        // <<plumber::Fragment(pestFunction)>>
		// <<plumber::Block(block)>>
        // default block
		// <</plumber::Block>>
        // <</plumber::Fragment>>
    }
`))
	assert.NilError(t, bw.Close())

	f, _ := op.Open("test.go")

	b, _ := io.ReadAll(f)

	expected := `
	import (
        // <<plumber::Fragment(import)>>
		// <<plumber::Block(customImports)>>
        1
		// <</plumber::Block>>
        // <</plumber::Fragment>>
	)


    func Test2() {
        // <<plumber::Fragment(pestFunction locked)>>
        locked
        // <</plumber::Fragment>>
    }
`
	assert.Equal(t, string(b), expected)
}

func TestFindBlocks(t *testing.T) {
	body := []byte(`
footer

/// Block-RPC(User)UserBody///EndBlock-RPC(User)

middle

///Block-RPC(Mailing)MailingBody///EndBlock-RPC(Mailing)

footer
`)
	blocks := FindBlocks("Block-RPC", body)
	assert.Equal(t, fmt.Sprintf("%q", blocks), `[{"User" "UserBody"} {"Mailing" "MailingBody"}]`)
}
