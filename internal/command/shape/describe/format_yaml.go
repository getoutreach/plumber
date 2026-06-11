// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the YAML output formatter for the describe command.

package describe

import "gopkg.in/yaml.v3"

// yamlFormatter implements the Formatter interface to render the Description in YAML format.
type yamlFormatter struct{}

func (yamlFormatter) Format(desc Description) ([]byte, error) {
	return yaml.Marshal(desc)
}
