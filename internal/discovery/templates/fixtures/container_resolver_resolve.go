//go:build plumber
// +build plumber

// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file is a fixture template used by the discovery system to generate container resolver resolution stubs.

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
