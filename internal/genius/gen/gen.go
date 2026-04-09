// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the template rendering engine including RenderContent, byte processors, render options, and utility functions for code generation.

// Package gen provides a template-based code generation engine with configurable byte processors, file I/O abstractions, and sprig-enhanced template rendering.
package gen

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"log"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"go/format"

	"golang.org/x/tools/imports"

	"github.com/Masterminds/sprig/v3"
	"github.com/fatih/color"
	"github.com/pkg/errors"
)

type TemplateFunc func(*template.Template) *template.Template

type PostProcessorFunc func(filename string) error

func (f PostProcessorFunc) Apply(filename string) error {
	return f(filename)
}

func (f PostProcessorFunc) Close() error {
	return nil
}

type PostProcessor interface {
	Apply(filename string) error
	Close() error
}

type PostProcessors []PostProcessor

func (pp PostProcessors) Apply(filepath string) error {
	for _, p := range pp {
		if err := p.Apply(filepath); err != nil {
			return err
		}
	}
	return nil
}

func (pp PostProcessors) Close() error {
	for _, p := range pp {
		if err := p.Close(); err != nil {
			return err
		}
	}
	return nil
}

type ByteProcessor func(filename string, content []byte) ([]byte, error)

type Config struct {
	FuncMap          template.FuncMap
	Package          string
	OutputDir        string
	ProtoDir         string
	ByteProcessors   []ByteProcessor
	GeneratorPackage string
}

func OutputDir(d string) *Config {
	return &Config{OutputDir: d}
}

func Package(p string) *Config {
	return &Config{
		Package:   p,
		OutputDir: "./" + p,
	}
}

var linePositionRe = regexp.MustCompile(`(\d+):(\d+):(.*)`)

var lineError = regexp.MustCompile(`//(:? )?ERROR: ?(.*)`)

var DebugProcessors = []ByteProcessor{
	func(filename string, content []byte) ([]byte, error) {
		if !strings.HasSuffix(filename, ".go") {
			return content, nil
		}
		matches := lineError.FindAllStringSubmatch(string(content), -1)
		for _, m := range matches {
			fmt.Println("ERROR:", m[2])
		}
		return content, nil
	},
}

var DefaultByteProcessors = []ByteProcessor{
	func(filename string, content []byte) ([]byte, error) {
		if !strings.HasSuffix(filename, ".go") {
			return content, nil
		}
		buf, err := format.Source(content)
		if err != nil {
			fmt.Println(displayError(string(content), err.Error()))
			//regexp.
			log.Fatal("format output:", err)
			return content, err
		}
		return buf, nil
	},
	func(filename string, content []byte) ([]byte, error) {
		if !strings.HasSuffix(filename, ".go") {
			return content, nil
		}

		buf, err := imports.Process(filename, content, nil)
		if err != nil {
			fmt.Println(displayError(string(content), err.Error()))
			log.Fatal("import processing:", err)
			return content, err
		}
		return buf, nil
	},
}

func displayError(content, err string) string {
	matches := linePositionRe.FindStringSubmatch(err)
	if len(matches) == 0 {
		return err + "NO"
	}
	line, _ := strconv.Atoi(matches[1])     //nolint: errcheck // Why: Regexp
	position, _ := strconv.Atoi(matches[2]) //nolint: errcheck // Why: Regexp
	err = matches[3]
	lines := strings.Split(content, "\n")

	red := color.New(color.FgRed)
	white := color.New(color.FgWhite)

	lines = append(lines[:line+1], lines[line:]...)
	lines[line] = white.Sprint(strings.Repeat("-", position-1)) + red.Sprint("^"+" # "+err)
	return strings.Join(lines, "\n")
}

type RenderOptions struct {
	TemplateFileName string
	TemplateName     string
	FuncMaps         []template.FuncMap

	TemplateFuncs   []func(*template.Template) *template.Template
	TemplateBundles []templateBundle
	Templates       []*template.Template
}

func (op *RenderOptions) Apply(t *template.Template) (*template.Template, error) {
	var err error
	for _, f := range op.FuncMaps {
		t.Funcs(f)
	}
	for _, f := range op.TemplateFuncs {
		t = f(t)
	}
	for _, b := range op.TemplateBundles {
		t, err = t.ParseFS(b.FS, b.Path)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("renderoption: parsefs [%s]", b.Path))
		}
	}
	for _, i := range op.Templates {
		t, err = t.AddParseTree(i.Name(), i.Tree)
		if err != nil {
			return nil, errors.Wrap(err, "renderoption: addparsetree")
		}
	}

	return t, err
}

type RenderOptionsFunc func(*RenderOptions) error

func (f RenderOptionsFunc) Apply(o *RenderOptions) error {
	return f(o)
}

func WithTemplateFunc(f func(*template.Template) *template.Template) RenderOptionsFunc {
	return RenderOptionsFunc(func(ro *RenderOptions) error {
		if f != nil {
			ro.TemplateFuncs = append(ro.TemplateFuncs, f)
		}
		return nil
	})
}

func WithTemplate(t *template.Template) RenderOptionsFunc {
	return RenderOptionsFunc(func(ro *RenderOptions) error {
		if t != nil {
			ro.Templates = append(ro.Templates, t)
		}
		return nil
	})
}

type templateBundle struct {
	FS   fs.FS
	Path string
}

func WithFS(f fs.FS, paths ...string) RenderOptionsFunc {
	return RenderOptionsFunc(func(ro *RenderOptions) error {
		for _, path := range paths {
			ro.TemplateBundles = append(ro.TemplateBundles, templateBundle{
				FS:   f,
				Path: path,
			})
		}
		return nil
	})
}

func WithFuncMap(f template.FuncMap) RenderOptionsFunc {
	return RenderOptionsFunc(func(ro *RenderOptions) error {
		ro.FuncMaps = append(ro.FuncMaps, f)
		return nil
	})
}

func WithRenderOptions(opts ...RenderOptionsFunc) RenderOptionsFunc {
	return RenderOptionsFunc(func(ro *RenderOptions) error {
		for _, o := range opts {
			if err := o.Apply(ro); err != nil {
				return err
			}
		}
		return nil
	})
}

type RenderOption interface {
	Apply(*RenderOptions) error
}

func RenderContent(
	ctx *Context,
	templateName string,
	output io.Writer,
	data interface{},
	opts ...RenderOption,
) error {
	renderOpts := &RenderOptions{
		FuncMaps:        []template.FuncMap{sprig.FuncMap()},
		TemplateBundles: []templateBundle{},
		Templates:       []*template.Template{},
	}
	for _, o := range opts {
		if err := o.Apply(renderOpts); err != nil {
			return err
		}
	}

	if reporter, ok := data.(interface {
		InflateContext(*Context)
	}); ok {
		reporter.InflateContext(ctx)
	}
	ctx.SetTemplateName(templateName)

	root := template.New("root")
	root = root.Funcs(template.FuncMap{
		"genus_context": func() *Context {
			return ctx
		},
		"genus_warn": func(message string) string {
			ctx.Warn(message)
			return ""
		},
		"genus_error": func(message string) string {
			ctx.Error(message)
			return ""
		},
		"genus_application_name":     func() string { return ctx.ApplicationName },
		"genus_current_package":      ctx.Package,
		"genus_current_package_name": ctx.PackageName,
		"genus_include_path": func(module string, fragments ...string) string {
			path := strings.Join(fragments, "")
			if strings.HasSuffix(module, "/") {
				module += "/"
			}
			path = strings.TrimPrefix(path, "/")
			path = strings.TrimSuffix(path, "/")
			newPath := ctx.RewriteFilename(path)
			if newPath != "" {
				newPath = "/" + newPath
			}
			return strings.NewReplacer(
				"{application}", ctx.ApplicationName,
			).Replace(fmt.Sprintf("%q", module+newPath))
		},
	})

	var (
		b    = &bytes.Buffer{}
		tmpl *template.Template
		err  error
	)

	tmpl, err = renderOpts.Apply(root)
	if err != nil {
		return fmt.Errorf("render options error in template:%q : %w", templateName, err)
	}

	err = tmpl.ExecuteTemplate(b, templateName, data)
	if err != nil {
		return fmt.Errorf("error during template %v rendering: %w", templateName, err)
	}

	_, err = output.Write(b.Bytes())
	return err
}

func Stringify(i interface{}) string {
	switch v := i.(type) {
	case uint64:
		return strconv.FormatUint(v, 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case string:
		return v
	case fmt.Stringer: // covers as well *uuid.UUID
		return v.String()
	default:
		panic(fmt.Sprintf("I don't know about type %T!\n", v))
	}
}

var title = color.New(color.BgHiWhite).Add(color.FgBlack).SprintFunc()
var component = color.New(color.BgWhite).Add(color.FgBlack).SprintFunc()

var size = 40

func Title(s string) FeatureFunc {
	return func(*Context, *Writer) error {
		fmt.Printf("%s\n", title(fmt.Sprintf(" %s", s+strings.Repeat(" ", size-len(s)))))
		return nil
	}
}

func Err(err error) FeatureFunc {
	return func(*Context, *Writer) error {
		return err
	}
}

func Component(s string) FeatureFunc {
	return func(*Context, *Writer) error {
		fmt.Printf("%s\n", component(fmt.Sprintf(" %s", s+strings.Repeat(" ", size-len(s)))))
		return nil
	}
}
