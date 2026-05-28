package plumber_test

import (
	"testing"

	"github.com/getoutreach/plumber"
	"gotest.tools/v3/assert"
)

func TestWrappers(t *testing.T) {
	c := newContainer("hello")

	assert.Equal(t, 1, c.Counter.Instance())

	restore, err := plumber.SavePoint(c, &c.Greeting)
	assert.NilError(t, err)

	t.Run("redefining a cloned dep does not affect the source", func(t *testing.T) {
		// Reset both Greeting and its transitive dependent (Sub.Derived) on the clone so
		// they can be re-defined and re-resolved independently of the original container.

		assert.Equal(t, "hello", c.Greeting.Instance())

		if err := plumber.ResetDefinition(c, &c.Greeting); err != nil {
			t.Fatalf("ResetDefinition on clone failed: %v", err)
		}
		// We set a wrapper on the new definition to verify that wrappers are applied correctly after resetting a definition.
		c.Greeting.Wrap(&c.GreetingWrapper).Const("howdy")

		assert.Equal(t, "wrapper[howdy]", c.Greeting.Instance())
		assert.Equal(t, "wrapper[howdy]!", c.Sub.Derived.Instance())
	})

	t.Run("Restore point does not affect the source", func(t *testing.T) {
		restore()
		assert.Equal(t, "hello", c.Greeting.Instance())
		assert.Equal(t, "hello!", c.Sub.Derived.Instance())
	})
}

func TestWrappersDisable(t *testing.T) {
	c := newContainer("hello")

	assert.Equal(t, 1, c.Counter.Instance())

	restore, err := plumber.SavePoint(c, &c.Greeting)
	defer restore()

	assert.NilError(t, err)

	t.Run("redefining a cloned dep does not affect the source", func(t *testing.T) {
		if err := plumber.ResetDefinition(c, &c.Greeting); err != nil {
			t.Fatalf("ResetDefinition on clone failed: %v", err)
		}

		err = plumber.DisableWrapper(c, &c.GreetingWrapper)
		assert.NilError(t, err)

		// We set a wrapper on the new definition to verify that wrappers are applied correctly after resetting a definition.
		c.Greeting.Wrap(&c.GreetingWrapper).Const("howdy")

		assert.Equal(t, "howdy", c.Greeting.Instance())
		assert.Equal(t, "howdy!", c.Sub.Derived.Instance())
	})
}
