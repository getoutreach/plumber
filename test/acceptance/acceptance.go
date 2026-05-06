// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides acceptance test helpers including fixture management, golden file
// comparison, and plumber shape invocation utilities.

// Package acceptance_test provides shared helpers for acceptance tests that verify plumber shape
// and derive code generation against golden fixtures.
package acceptance_test

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"

	"github.com/getoutreach/plumber/internal/command/discovery/render"
	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/report/term"
	"github.com/getoutreach/plumber/internal/command/shape/structure"
	"github.com/getoutreach/plumber/internal/command/template"
	"gotest.tools/v3/assert"
)

//go:embed fixture/**
var fixtures embed.FS

type FixtureContext struct {
	BaseDir        string
	ShapingContext *contract.ShapingContext
	Cfg            *shape.Config
}

func withFixture(cfg *shape.Config, fn func(ctx FixtureContext) error, files ...string) error {
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current working directory: %w", err)
	}

	baseDir, err := os.MkdirTemp("./", "testrun-acceptance")
	if err != nil {
		return err
	}
	tmpDir := baseDir
	baseDir = path.Join(baseDir, "fixture")
	if err := os.MkdirAll(baseDir, 0o777); err != nil {
		return err
	}
	defer func() {
		err := os.Chdir(wd)
		if err != nil {
			fmt.Printf("failed to change directory back to original: %v\n", err)
		}
		fmt.Println("removing ", tmpDir)
		err = os.RemoveAll(tmpDir)
		if err != nil {
			fmt.Printf("failed to remove temp dir: %v\n", err)
		}
	}()
	if err := os.Chdir(baseDir); err != nil {
		return err
	}

	mod := strings.TrimPrefix(baseDir, wd)
	mod = path.Join("github.com/getoutreach/plumber/test/acceptance", mod)

	shapingContext := newShapingContext(cfg)
	shapingContext.Module = contract.ModuleInfo{
		Path: mod,
		Name: path.Base(mod),
		Dir:  path.Join(wd, baseDir),
	}

	c := FixtureContext{BaseDir: baseDir, ShapingContext: shapingContext, Cfg: cfg}

	for _, file := range files {
		dirName := path.Dir(file)
		if err := os.MkdirAll(dirName, 0o777); err != nil {
			return fmt.Errorf("failed to create directory %q: %w", dirName, err)
		}

		src, err := fixtures.Open(path.Join("fixture", file))
		if err != nil {
			return fmt.Errorf("can't open src %s: %w", file, err)
		}

		dst, err := os.Create(file)
		if err != nil {
			return fmt.Errorf("can't create dst %s: %w", file, err)
		}

		_, err = io.Copy(dst, src)
		if err != nil {
			return err
		}

		err = errors.Join(dst.Close(), src.Close())
		if err != nil {
			return err
		}
	}

	return fn(c)
}

func (ctx FixtureContext) AssertContent(t *testing.T, filename, expected string) {
	content, err := os.ReadFile(filename)
	assert.NilError(t, err)

	expectedContent, err := fixtures.ReadFile(path.Join("fixture", "@golden", expected))
	assert.NilError(t, err)

	re := regexp.MustCompile(`testrun-acceptance[a-z0-9]+/`)

	content = re.ReplaceAll(content, []byte("testrun-acceptance/"))

	assert.Equal(t, string(expectedContent), string(content))
}

func newShapingContext(cfg *shape.Config) *contract.ShapingContext {
	tl := template.NewTemplateCache(cfg.Templates.Sources, cfg.Templates.Content, "/tmp", render.EmbededTemplates)

	ctx := contract.NewShapingContext(
		context.Background(), term.NewTerminalReporter(), tl, &structure.NoopResolver{})
	ctx.Module = contract.ModuleInfo{
		Path: "github.com",
	}
	return ctx
}
