#!/bin/sh
go run ../cmd/plumber/plumber.go shape describe functions -c plumber.shape.yaml >docs/functions.md
