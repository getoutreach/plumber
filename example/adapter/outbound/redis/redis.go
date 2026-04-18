// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: database infra for example application

// Package redis provides interface to Redis for example application
package redis

// Client represents a Redis client
type Client struct {
}

func NewClient() (*Client, error) {
	return &Client{}, nil
}
