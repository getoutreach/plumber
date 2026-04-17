// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Tests for provider name extraction from constructors
// Managed: true

package discovery_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/getoutreach/plumber/internal/discovery"
	"gotest.tools/v3/assert"
)

// testFixtureDir creates a temporary fixture directory inside the current
// module tree so that go/packages can load it. Returns the directory path.
func testFixtureDir(t *testing.T, name string) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("testdata", name))
	assert.NilError(t, err)
	err = os.MkdirAll(dir, 0o755)
	assert.NilError(t, err)
	t.Cleanup(func() { os.RemoveAll("testdata") })
	return dir
}

func TestConstructorProviderExtraction(t *testing.T) {
	dir := testFixtureDir(t, "provider_extraction")

	serviceGoContent := `package provider_extraction

// Service is a test service
type Service struct {
	Name string
}

// NewService creates a new service - provider name should be "Service"
func NewService(name string) *Service {
	return &Service{Name: name}
}

// Repository is a data repository
type Repository struct {
	DB string
}

// FactoryRepository creates a new repository - provider name should be "Repository"
func FactoryRepository(db string) *Repository {
	return &Repository{DB: db}
}

// Helper is not a constructor (doesn't match pattern)
func Helper() string {
	return "help"
}
`
	err := os.WriteFile(filepath.Join(dir, "service.go"), []byte(serviceGoContent), 0o644)
	assert.NilError(t, err)

	parser, err := discovery.NewASTParser(filepath.Join(dir, "service.go"))
	assert.NilError(t, err)

	// Test with constructor matcher using named capture groups
	matchers := []discovery.Matcher{
		{
			Constructors: []string{
				`New(?P<name>.*)`,     // Matches NewService
				`Factory(?P<name>.*)`, // Matches FactoryRepository
			},
		},
	}

	result, err := parser.Discover(matchers)
	assert.NilError(t, err)

	// Should find 2 providers matching the patterns
	assert.Equal(t, len(result.Providers), 2, "should find 2 providers")

	// Verify first provider
	assert.Equal(t, result.Providers[0].Name, "Service")
	assert.Assert(t, result.Providers[0].Type != nil, "provider should have type")
	assert.Assert(t, result.Providers[0].Constructor != nil, "Service should have a constructor")
	assert.Equal(t, result.Providers[0].Constructor.FunctionName, "NewService")

	// Verify second provider
	assert.Equal(t, result.Providers[1].Name, "Repository")
	assert.Assert(t, result.Providers[1].Type != nil, "provider should have type")
	assert.Assert(t, result.Providers[1].Constructor != nil, "Repository should have a constructor")
	assert.Equal(t, result.Providers[1].Constructor.FunctionName, "FactoryRepository")
}

func TestConstructorWithoutNamedGroup(t *testing.T) {
	dir := testFixtureDir(t, "no_named_group")

	serviceGoContent := `package no_named_group

// Service is a test service
type Service struct {
	Name string
}

// NewService creates a new service
func NewService(name string) *Service {
	return &Service{Name: name}
}
`
	err := os.WriteFile(filepath.Join(dir, "service.go"), []byte(serviceGoContent), 0o644)
	assert.NilError(t, err)

	parser, err := discovery.NewASTParser(filepath.Join(dir, "service.go"))
	assert.NilError(t, err)

	// Test with pattern without named capture group
	matchers := []discovery.Matcher{
		{
			Constructors: []string{
				`New.*`, // Matches but no capture group
			},
		},
	}

	result, err := parser.Discover(matchers)
	assert.NilError(t, err)

	// Should not find any providers without named capture group
	assert.Equal(t, len(result.Providers), 0, "should not find providers without named capture group")
}
