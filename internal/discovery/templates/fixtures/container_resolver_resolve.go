//go:build plumber
// +build plumber

package templates

import "github.com/getoutreach/plumber"

func DependencyResolverResolve() {
	c.NAME.Resolver(func(r *plumber.Resolution[DEPENDANCY_TYPE]) {
		r.Require().Then(func() {
			r.RESOLVE(
				CONSTRUCTOR_FUNCTION,
			)
		})
	})
}
