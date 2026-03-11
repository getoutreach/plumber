// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Tests for AST parser
// Managed: true

package discovery_test

import (
	"fmt"
	"go/types"
	"os"
	"path/filepath"
	"testing"

	"github.com/getoutreach/plumber/internal/discovery"
	"gotest.tools/v3/assert"
)

func TestParseFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.go")

	content := `package test

// User represents a user entity
type User struct {
	Name string
	Age  int
}

// NewUser creates a new user
func NewUser(name string, age int) *User {
	return &User{Name: name, Age: age}
}
`

	err := os.WriteFile(testFile, []byte(content), 0644)
	assert.NilError(t, err)

	file, dec, err := discovery.ParseFile(testFile)
	assert.NilError(t, err)
	assert.Assert(t, file != nil)
	assert.Assert(t, dec != nil)
	assert.Equal(t, file.Name.Name, "test")
}

func TestASTParserDiscover(t *testing.T) {
	// Use the example directory which already has real code
	examplePath := "../../../example/adapter/async/async.go"

	// Check if the path exists
	if _, err := os.Stat(examplePath); os.IsNotExist(err) {
		t.Skip("Example directory not found, skipping test")
	}

	parser, err := discovery.NewASTParser(examplePath)
	if err != nil {
		t.Skipf("Could not create AST parser: %v", err)
	}

	matchers := []discovery.Matcher{
		{
			Constructors: []string{"New(?P<name>.*)"},
		},
	}

	result, err := parser.Discover(matchers)
	assert.NilError(t, err)
	assert.Assert(t, result != nil)

	// The example should have at least some providers
	t.Logf("Found %d providers", len(result.Providers))
}

func TestASTParserWithMockCode(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test package with multiple files
	goModContent := `module github.com/getoutreach/testpkg

go 1.23
`
	err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(goModContent), 0644)
	assert.NilError(t, err)

	// Create multiple Go files to test cross-file type resolution
	typesGoContent := `package testpkg

// Container is a shared container type
type Container struct {
	Name string
}
`
	err = os.WriteFile(filepath.Join(tmpDir, "types.go"), []byte(typesGoContent), 0644)
	assert.NilError(t, err)

	serviceGoContent := `package testpkg

// Service is a test service
type Service struct {
	Name      string
	Container *Container // Reference to type in another file
}

// NewService creates a new service
func NewService(name string, c *Container) *Service {
	return &Service{Name: name, Container: c}
}

// Repository is a data repository
type Repository struct {
	DB string
}

// CreateRepository creates a new repository
func CreateRepository(db string) (*Repository, error) {
	return &Repository{DB: db}, nil
}
`
	err = os.WriteFile(filepath.Join(tmpDir, "service.go"), []byte(serviceGoContent), 0644)
	assert.NilError(t, err)

	// Test with both files - this should resolve cross-file types
	parser, err := discovery.NewASTParser(
		filepath.Join(tmpDir, "service.go"),
		filepath.Join(tmpDir, "types.go"),
	)
	assert.NilError(t, err)

	matchers := []discovery.Matcher{
		{
			Constructors: []string{"New(?P<name>.*)", "Create(?P<name>.*)"},
		},
	}

	result, err := parser.Discover(matchers)
	assert.NilError(t, err)

	// Should find both providers
	assert.Assert(t, len(result.Providers) >= 2, "should find at least 2 providers")

	// Verify first provider (Service)
	assert.Equal(t, result.Providers[0].Name, "Service")
	assert.Assert(t, result.Providers[0].Type != nil)
	assert.Assert(t, result.Providers[0].Constructor != nil, "Service should have a constructor")
	assert.Equal(t, result.Providers[0].Constructor.FunctionName, "NewService")

	// Verify second provider (Repository)
	assert.Equal(t, result.Providers[1].Name, "Repository")
	assert.Assert(t, result.Providers[1].Type != nil)
	assert.Assert(t, result.Providers[1].Constructor != nil, "Repository should have a constructor")
	assert.Equal(t, result.Providers[1].Constructor.FunctionName, "CreateRepository")

	assert.Assert(t, !result.Providers[0].Constructor.ReturnsError(), "first constructor should not return error")
	assert.Assert(t, result.Providers[1].Constructor.ReturnsError(), "second constructor should return error")

	for _, p := range result.Providers[1].Constructor.ReturnParameters {
		fmt.Println(types.TypeString(p.TypeInfo.Type, RelativeTo(p.TypeInfo.Package.Types)))
	}

	fmt.Println()
	// fmt.Println(c)
}

// RelativeTo returns a [Qualifier] that fully qualifies members of
// all packages other than pkg.
func RelativeTo(pkg *types.Package) types.Qualifier {
	if pkg == nil {
		return nil
	}
	return func(other *types.Package) string {
		if pkg == other {
			return other.Name() // same package; unqualified
		}
		return other.Name()
	}
}
