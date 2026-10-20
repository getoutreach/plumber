#!/bin/sh
go run ../cmd/plumber/plumber.go shape -c plumber.shape.yaml describe functions >docs/functions.md
