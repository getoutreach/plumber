// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file define an expansion of transformer output
package expand

import (
	"path"
	"regexp"
	"strings"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/query/model"
)

// reSuffixed is a regular expression used to identify and replace {suffix:...} patterns in output filenames for generated code.
var reSuffixed = regexp.MustCompile(`{suffix:([^}]+)}`)

func TransformerOutput(annotations model.Annotations, fileName string) string {
	output := "generated.go"

	a := annotations.Find(contract.OptionOutput)

	if a != nil {
		output = a.Value()
	}
	baseFilename := path.Base(fileName)
	ext := path.Ext(baseFilename)

	name := strings.TrimSuffix(baseFilename, ext)

	output = strings.NewReplacer(
		"{filename}", baseFilename,
		"{name}", name,
		"{ext}", ext,
	).Replace(output)

	output = reSuffixed.ReplaceAllStringFunc(output, func(s string) string {
		// Extract the suffix value from the match
		matches := reSuffixed.FindStringSubmatch(s)
		if len(matches) > 1 {
			suffix := matches[1]
			return name + "_" + suffix + ext
		}
		return s
	})

	return output
}
