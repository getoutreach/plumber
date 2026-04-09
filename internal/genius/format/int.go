// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides integer conversion utilities for format transformations.

package format

import "strconv"

func Int32ToString(i int32) string {
	return strconv.FormatInt(int64(i), 10)
}
