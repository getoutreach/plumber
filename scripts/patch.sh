#!/bin/bash

# update linter settings
index=$(mise exec -- yq '.lintroller.exclusions.paths | .[] |  select(. == "example") | .' golangci.yml)
if [[ $index == "" ]]; then
  mise exec -- yq -i '.lintroller.exclusions.paths |= . + ["test/acceptance/fixture", "test/acceptance/generated", "example"]' golangci.yml
fi
