// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: The template functions used during the expand phase of the shape command.
package expand

import (
	"errors"
	"fmt"
	"path"

	"github.com/getoutreach/plumber/query/model"
	"github.com/samber/lo"
)

func filenameSuffixed(c *EvaluationContext) any {
	return func(suffix string) string {
		pos := c.node.GetPosition()
		output := toOutputTemplateData(path.Join(c.node.GetPackage().Dir, pos.Filename))
		return fmt.Sprintf("%s_%s%s", output.Name, suffix, output.Ext)
	}
}

func pathJoin(c *EvaluationContext) any {
	return func(parts ...string) (string, error) {
		var err []error
		parts = lo.Map(parts, func(o string, _ int) string {
			p, e := c.structurePathResolver.ResolveStructurePath(o)
			err = append(err, e)
			return p
		})
		p := path.Join(parts...)
		fmt.Println("JOIN", p)
		return p, errors.Join(err...)
	}
}
func macroDefaultsName(c *EvaluationContext) any {
	return func(other ...string) (string, error) {
		if len(c.data.Source.Args) > 0 {
			return c.data.Source.Args[0], nil
		}
		if c.data.Type != nil {
			if t, ok := c.data.Type.(*model.Type); ok {
				return t.Name, nil
			}
		}
		if len(other) > 0 {
			return other[0], nil
		}
		return "", errors.New("no default name available")
	}
}
