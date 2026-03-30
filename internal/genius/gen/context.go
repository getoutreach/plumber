package gen

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/getoutreach/plumber/internal/genius/pathutil"
)

type FilenameRewriter func(string) (bool, string)

type contextSettings struct {
	Filename string
}

type contextError struct {
	message string
}

type Context struct {
	ApplicationName    string
	OutputDir          string
	ApplicationPackage string
	metadata           map[string]any
	rewrites           []FilenameRewriter
	stack              []contextSettings
	logger             *Logger
	warn               []contextError
	errors             []contextError
}

func (c *Context) Reset() {
	c.metadata = map[string]any{}
	c.warn = []contextError{}
}

func (c *Context) SetTemplateName(v string) {
	c.metadata["template"] = v
}

func (c *Context) SetResource(v string) {
	c.metadata["resource"] = v
}

func (c *Context) Metadata(m map[string]any) {
	for k, v := range m {
		c.metadata[k] = v
	}
}

func (c *Context) Warn(message string) {
	c.warn = append(c.warn, contextError{
		message: message,
	})
}

func (c *Context) Error(message string) {
	c.errors = append(c.errors, contextError{
		message: message,
	})
}

func NewContext(applicationName string, opts ...func(*Context)) *Context {
	c := &Context{
		ApplicationName: applicationName,
		logger:          NewLogger(),
	}

	c.rewrites = []FilenameRewriter{}

	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Context) Filename() string {
	return c.current().Filename
}

func (c *Context) Package() string {
	parts := []string{
		strings.TrimSuffix(c.ApplicationPackage, "/"),
	}
	dir := strings.TrimPrefix(c.OutputDir, "./")
	if dir != "" {
		parts = append(parts, dir)
	}
	parts = append(parts, path.Dir(c.current().Filename))
	return strings.Join(parts, "/")
}

func (c *Context) PackageName() string {
	dir := path.Dir(c.current().Filename)
	return path.Base(dir)
}

func (c *Context) current() contextSettings {
	if len(c.stack) == 0 {
		return contextSettings{}
	}
	return c.stack[len(c.stack)-1]
}

func (c *Context) with(settings contextSettings, callback func() error) error {
	c.stack = append(c.stack, settings)
	err := callback()
	c.stack = c.stack[:len(c.stack)-1]
	return err
}

func (c *Context) Write(wr *Writer, filename string, openFileFunc func(*Context, io.Writer) error, opts ...WriterOption) error {
	wc := WriterConfig{}.Apply(opts...)
	var err error
	filename, err = c.rewriteFilenameAndMigrate(&wc, filename)
	if err != nil {
		return fmt.Errorf("can't rewrite filename: %w", err)
	}
	return c.with(contextSettings{
		Filename: filename,
	}, func() error {
		err := wr.Write(c,
			filename, func(w io.Writer) error {
				return openFileFunc(c, w)
			}, opts...)
		c.logger.GenerationReport(c, filename, err)
		return err
	})
}

func (c *Context) RewriteFilename(filename string) string {
	return strings.NewReplacer("{application}", c.ApplicationName).Replace(rewriteFilename(filename, c.rewrites...))
}

func (c *Context) rewriteFilenameAndMigrate(wc *WriterConfig, filename string) (string, error) {
	newFilename := c.RewriteFilename(filename)
	if newFilename != filename {
		oldfullPath := path.Join(wc.OutputDir, filename)
		newFullPath := path.Join(wc.OutputDir, newFilename)
		if pathutil.MustExists(oldfullPath) && !pathutil.MustExists(newFullPath) {
			err := os.MkdirAll(path.Dir(newFullPath), os.ModePerm)
			if err != nil {
				return filename, fmt.Errorf("can't ensure directory: %w", err)
			}
			cmd := exec.Command("git", "mv", oldfullPath, newFullPath)
			output, err := cmd.CombinedOutput()
			if err != nil {
				fmt.Println(string(output))
				return filename, fmt.Errorf("can't rename file: %w", err)
			}
		}
		return newFilename, nil
	}
	return filename, nil
}

func rewriteFilename(filename string, rewriters ...FilenameRewriter) string {
	var (
		finished bool
	)
	for _, f := range rewriters {
		if finished, filename = f(filename); finished {
			return filename
		}
	}
	return filename
}
