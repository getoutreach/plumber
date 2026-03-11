package templates

import (
	"bytes"
	"fmt"
	"go/format"
	"go/token"
	"strings"
	"testing"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/dave/dst/decorator/resolver/gopackages"
	"github.com/dave/dst/dstutil"
)

func TestContainerResolverResolveAst(t *testing.T) {
	n := ContainerResolver(
		SelectorExprNameReplace(map[string]string{
			"NAME":               "Async",
			"DEPENDANCY_PACKAGE": "async",
			"RESOLVE":            "ResolveError",
		}),
		IdentReplace(map[string]any{
			"DEPENDANCY_TYPE": "AsyncDependency",
			"CONSTRUCTOR_FUNCTION": func(c *dstutil.Cursor) {
				c.Replace(&dst.Ident{Path: "fmt", Name: "Println"})
			},
		}),
	)

	// Create a restorer with the import manager enabled, and print the result. As you can see, the
	// import block is automatically managed, and the Println ident is converted to a SelectorExpr:
	r := decorator.NewRestorerWithImports("root", gopackages.New("."))
	restoredFile, err := r.RestoreFile(n)

	//restoredFset, restoredFile, err := decorator.RestoreFile(n)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := format.Node(&buf, r.Fset, restoredFile); err != nil {
		t.Fatal(err)
	}

	fmt.Println(buf.String())

	return

	b := strings.Builder{}

	format.Node(&b, token.NewFileSet(), n)

	fmt.Println(b.String())
}
