package plumber_test

import (
	"testing"

	"github.com/getoutreach/plumber"
	"gotest.tools/v3/assert"
)

func TestAs(t *testing.T) {
	type Dep struct {
		Value string
	}

	type ContainerA struct {
		Dependency1 plumber.D[*Dep]
		Dependency2 plumber.D[*Dep]
	}

	type Root struct {
		plumber.Container
		ContainerA *ContainerA
	}

	root := &Root{
		ContainerA: new(ContainerA),
	}

	root.ContainerA.Dependency1.Define(func() *Dep {
		return &Dep{Value: "test"}
	})

	root.ContainerA.Dependency2.As(&root.ContainerA.Dependency1)

	t.Run("Instance is derived successfully", func(t *testing.T) {
		assert.Equal(t, "test", root.ContainerA.Dependency2.Instance().Value)
	})

	t.Run("Instance is distinct", func(t *testing.T) {
		d1 := root.ContainerA.Dependency1.Instance()
		d2 := root.ContainerA.Dependency2.Instance()
		assert.Assert(t, d1 != d2)
	})
}
