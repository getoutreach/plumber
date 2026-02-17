// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Discovery CLI tool
// Managed: true

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/getoutreach/plumber/cmd/plumber/discovery"
)

func main() {
	ctx := context.Background()

	if err := discovery.Run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
