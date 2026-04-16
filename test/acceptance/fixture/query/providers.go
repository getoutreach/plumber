// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines provider functions used as query targets in the plumber:query acceptance test fixture.

// Package query provides fixture types and functions for acceptance testing of plumber:query annotation processing.
package query

// InitDB initializes the database connection.
func InitDB() {}

// InitCache initializes the cache layer.
func InitCache() {}

// InitLogger initializes the logger.
func InitLogger() {}

// ShutdownDB gracefully shuts down the database connection.
func ShutdownDB() {}

// StartWorker starts a background worker (not matching Init pattern, different signature).
func StartWorker(name string) {}
