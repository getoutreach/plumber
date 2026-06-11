// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the JSON output formatter for the describe command.

package describe

import "encoding/json"

// jsonFormatter implements the Formatter interface to render the Description in JSON format.
type jsonFormatter struct{}

func (jsonFormatter) Format(desc Description) ([]byte, error) {
	return json.MarshalIndent(desc, "", "  ")
}
