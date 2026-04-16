// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file declares a plumber:query annotated variable that queries functions from a sibling package for cross-package acceptance testing.

package querycross

// plumber:query "^Init.*" scope="./providers"
var InitFunctions = []func(){}
