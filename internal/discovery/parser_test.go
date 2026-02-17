// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Tests for configuration parsing
// Managed: true

package discovery_test

import (
	"testing"

	"github.com/getoutreach/plumber/internal/discovery"
	"gotest.tools/v3/assert"
)

func TestParseConfig(t *testing.T) {
	yamlContent := `
applications:
  - name: testapp
    containers:
      - plumber.container:
          name: TestContainer
          container:
            path: ./test.go
`

	cfg, err := discovery.ParseConfigBytes([]byte(yamlContent))
	assert.NilError(t, err)
	assert.Equal(t, len(cfg.Applications), 1)
	assert.Equal(t, cfg.Applications[0].Name, "testapp")
	assert.Equal(t, len(cfg.Applications[0].Containers), 1)
	assert.Assert(t, cfg.Applications[0].Containers[0].PlumberContainer != nil)
	assert.Equal(t, cfg.Applications[0].Containers[0].PlumberContainer.Name, "TestContainer")
}

func TestParseConfigWithMatchers(t *testing.T) {
	yamlContent := `
applications:
  - name: testapp
    containers:
      - plumber.container:
          name: TestContainer
          container:
            path: ./test.go
          matchers:
            - plumber.matcher.struct:
                constructors:
                  - New{{ name }}
                  - Create{{ name }}
`

	cfg, err := discovery.ParseConfigBytes([]byte(yamlContent))
	assert.NilError(t, err)
	assert.Equal(t, len(cfg.Applications[0].Containers[0].PlumberContainer.Matchers), 1)

	matcher := cfg.Applications[0].Containers[0].PlumberContainer.Matchers[0]
	assert.Assert(t, matcher.PlumberMatcherStruct != nil)
	assert.Equal(t, len(matcher.PlumberMatcherStruct.Constructors), 2)
	assert.Equal(t, matcher.PlumberMatcherStruct.Constructors[0], "New{{ name }}")
}

func TestParseConfigWithLoop(t *testing.T) {
	yamlContent := `
applications:
  - name: testapp
    containers:
      - plumber.container:
          name: "{{ .module }}"
          container:
            path: ./{{ .module }}.go
        loop:
          path: ./adapter/(?P<module>\w+)/
`

	cfg, err := discovery.ParseConfigBytes([]byte(yamlContent))
	assert.NilError(t, err)

	container := cfg.Applications[0].Containers[0]
	assert.Assert(t, container.Loop != nil)
	assert.Equal(t, container.Loop.Path, "./adapter/(?P<module>\\w+)/")
}

func TestValidateConfigMissingName(t *testing.T) {
	yamlContent := `
applications:
  - containers:
      - plumber.container:
          container:
            path: ./test.go
`

	_, err := discovery.ParseConfigBytes([]byte(yamlContent))
	assert.ErrorContains(t, err, "name is required")
}

func TestValidateConfigNoContainers(t *testing.T) {
	yamlContent := `
applications:
  - name: testapp
    containers: []
`

	_, err := discovery.ParseConfigBytes([]byte(yamlContent))
	assert.ErrorContains(t, err, "no containers defined")
}
