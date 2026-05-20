// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the logic to expand handler variants in the shape configuration,
// allowing for more flexible handler definitions.
package expand

import (
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/samber/lo"
)

func Handlers(handlers []config.HandlerConfig) []config.HandlerConfig {
	expanded := make([]config.HandlerConfig, 0, len(handlers))
	for _, h := range handlers {
		if h.PlumberHandler != nil {
			expanded = append(expanded, h)
			for _, v := range h.PlumberHandler.Variants {
				expanded = append(expanded, config.HandlerConfig{
					PlumberHandler: &config.PlumberHandlerConfig{
						Name:    h.PlumberHandler.Name + "[" + v.Name + "]",
						Command: lo.Ternary(v.Command != "", v.Command, h.PlumberHandler.Command),
						Args:    lo.Ternary(len(v.Args) > 0, v.Args, h.PlumberHandler.Args),
					},
				})
			}
		}
	}
	return expanded
}
