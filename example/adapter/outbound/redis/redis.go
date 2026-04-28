// Copyright 2024 Outreach Corporation. All Rights Reserved.
// Description: database infra for example application

// Package redis provides interface to Redis for example application
package redis

// Client represents a Redis client
type Client struct {
}

type Dep1 struct {
}

// is:provider
func NewDep1() (*Dep1, error) {
	return &Dep1{}, nil
}

// is:provider
func NewDep1Named() (*Dep1, error) {
	return &Dep1{}, nil
}

// is:provider
func NewClient(*Dep1) (*Client, error) {
	return &Client{}, nil
}
