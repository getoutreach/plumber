package plumber_test

import (
	"testing"

	"github.com/getoutreach/plumber"
	"gotest.tools/v3/assert"
)

// subContainer is a nested container used to verify recursive cloning.
type subContainer struct {
	// Derived depends on Greeting from the parent container.
	Derived plumber.D[string]
}

// container is a small container used to exercise plumber.Clone.
type container struct {
	plumber.Container

	GreetingWrapper plumber.D[func(string) string]

	Greeting plumber.D[string]
	Count    plumber.D[int]
	Counter  plumber.D[int]
	Sub      subContainer
}

// newContainer builds and wires a container instance with the supplied greeting.
func newContainer(greeting string) *container {
	var counter int
	c := &container{}
	c.Greeting.Named("Greeting").Const(greeting)
	c.Count.Named("Count").Const(42)
	c.Counter.Named("Counter").Define(func() int {
		counter++
		return counter
	})
	c.GreetingWrapper.Named("GreetingWrapper").Const(func(s string) string {
		return "wrapper[" + s + "]"
	})
	c.Sub.Derived.Named("Sub.Derived").Resolver(func(r *plumber.Resolution[string]) {
		r.Require(&c.Greeting, &c.Counter).Then(func() {
			r.Resolve(c.Greeting.Instance() + "!")
		})
	})
	return c
}

func TestResetDefinitionSavePoint(t *testing.T) {
	c := newContainer("hello")

	assert.Equal(t, 1, c.Counter.Instance())

	// Save point creates a restoration point
	restore, err := plumber.SavePoint(c, &c.Greeting)
	assert.NilError(t, err)

	t.Run("redefining a cloned dep does not affect the source", func(t *testing.T) {
		// Reset both Greeting and its transitive dependent (Sub.Derived) on the clone so
		// they can be re-defined and re-resolved independently of the original container.
		if err := plumber.ResetDefinition(c, &c.Greeting); err != nil {
			t.Fatalf("ResetDefinition on clone failed: %v", err)
		}

		c.Greeting.Const("howdy")

		assert.Equal(t, "howdy", c.Greeting.Instance())
		assert.Equal(t, "howdy!", c.Sub.Derived.Instance())
	})

	t.Run("Restore point does not affect the source", func(t *testing.T) {
		// Restores to previous state
		restore()
		assert.Equal(t, "hello", c.Greeting.Instance())
		assert.Equal(t, "hello!", c.Sub.Derived.Instance())
	})
}
