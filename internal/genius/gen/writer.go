// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements Writer, ByteProcessingWriter, BlockWriter, and MemoryFileOpener
// for file I/O and plumber-block-aware content merging.

package gen

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
)

// DefaultWriter is a default instance of Writer with default byte processors, providing a convenient
// way to write content with standard processing applied.
var DefaultWriter = NewWriter(WithByteProcessors(DefaultByteProcessors))

// ByteProcessingWriter is an io.WriteCloser that buffers written bytes and applies a series of ByteProcessors to the content
type ByteProcessingWriter struct {
	buf            bytes.Buffer
	postProcessors []ByteProcessor
	fileName       string
	writer         io.Writer
}

func NewByteProcessingWriter(fileName string, postProcessors []ByteProcessor, w io.Writer) *ByteProcessingWriter {
	return &ByteProcessingWriter{
		fileName:       fileName,
		postProcessors: postProcessors,
		writer:         w,
	}
}

func (w *ByteProcessingWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *ByteProcessingWriter) Close() error {
	var (
		buf = w.buf.Bytes()
		err error
	)
	for _, postProcessor := range w.postProcessors {
		buf, err = postProcessor(w.fileName, buf)
		if err != nil {
			return err
		}
	}
	_, err = w.writer.Write(buf)
	return err
}

// reParams is a regular expression used to parse parameters from block headers,
// allowing for structured handling of parameters
var reParams = regexp.MustCompile(`([^=]+)=?([^ ]+)?`)

// BlockContent represents the content of a block identified by a name, allowing for structured handling of blocks
// in the generated content.
type BlockContent struct {
	Name    []byte
	Content []byte
}

// Element represents a single block or fragment parsed from the file content, allowing for structured handling
// of different types of content blocks.
type Element struct {
	Header        []byte
	CommentPrefix []byte
	Name          []byte
	Content       []byte
	Params        FragmentParams
}

// Elements represents a collection of blocks and fragments parsed from the file content,
// allowing for structured handling of different types of content blocks in the generated files.
type Elements struct {
	Blocks    []Element
	Fragments []Element
}

// Constants for default block names and patterns used in the block writer for identifying and
// preserving blocks in generated content.
const (
	// DefaultBlockName is the default pattern used to identify blocks in the generated content,
	// allowing for dynamic content insertion while preserving existing code structure.
	DefaultBlockName = "Block-API"
)

// FileOpener is an interface that abstracts file operations, allowing for different implementations
// such as in-memory or system file operations.
type FileOpener interface {
	MkdirAll(string, os.FileMode) error
	Open(string) (io.ReadWriteCloser, error)
	Create(string) (io.ReadWriteCloser, error)
	Content(string) []byte
}

// MemoryFileOpener is a FileOpener that operates on in-memory buffers, allowing for file-like operations
type MemoryFileOpener interface {
	FileOpener
}

func NewSystemFileOpener() FileOpener {
	return &SystemFileOpener{}
}

// SystemFileOpener is a FileOpener that interacts with the actual file system, allowing for reading, writing,
// and directory creation operations on real files and directories.
type SystemFileOpener struct{}

func (s *SystemFileOpener) Open(filename string) (io.ReadWriteCloser, error) {
	return os.Open(filename)
}

func (s *SystemFileOpener) Content(filename string) []byte {
	b, err := os.ReadFile(filename)
	if err != nil {
		panic(fmt.Sprintf("Was not able to read file content: %v", err))
	}
	return b
}

func (s *SystemFileOpener) Create(filename string) (io.ReadWriteCloser, error) {
	return os.Create(filename)
}

func (s *SystemFileOpener) MkdirAll(dirPath string, perm os.FileMode) error {
	return os.MkdirAll(dirPath, perm)
}

// BufferFile is an in-memory implementation of io.ReadWriteCloser, allowing for file-like operations on a
// byte buffer without interacting with the actual file system.
type BufferFile struct {
	*bytes.Buffer
}

func (bf *BufferFile) Close() error {
	return nil
}

// BufferFileOpener is a FileOpener that operates on in-memory buffers, allowing for file-like operations
// without interacting with the actual file system,
type BufferFileOpener struct {
	buffers   map[string]*BufferFile
	Filenames []string
}

func NewBufferFileOpener() *BufferFileOpener {
	return &BufferFileOpener{
		buffers: map[string]*BufferFile{},
	}
}

func (o *BufferFileOpener) Open(filename string) (io.ReadWriteCloser, error) {
	var bb []byte
	if b, ok := o.buffers[filename]; ok {
		bb = b.Bytes()
	} else {
		bb = make([]byte, 0)
	}
	f := &BufferFile{Buffer: bytes.NewBuffer(bb)}
	o.buffers[filename] = f
	return f, nil
}

func (o *BufferFileOpener) Create(filename string) (io.ReadWriteCloser, error) {
	f := &BufferFile{Buffer: bytes.NewBuffer(make([]byte, 0))}
	o.buffers[filename] = f
	return f, nil
}

func (o *BufferFileOpener) Write(filename string, content []byte) error {
	f := &BufferFile{Buffer: bytes.NewBuffer(content)}
	o.buffers[filename] = f
	o.Filenames = append(o.Filenames, filename)
	return nil
}

func (o *BufferFileOpener) MkdirAll(dirPath string, perm os.FileMode) error {
	return nil
}

func (o *BufferFileOpener) Content(filename string) []byte {
	if b, ok := o.buffers[filename]; ok {
		return b.Bytes()
	}
	return nil
}

// ReadOnlyFileOpener is a FileOpener that allows reading from the file system but writes to an in-memory buffer,
// enabling a read-only view of the file system while still supporting file creation and writing for
// generated content without affecting the actual file system.
type ReadOnlyFileOpener struct {
	fs     *SystemFileOpener
	memory *BufferFileOpener
}

func NewReadOnlyFileOpener() *ReadOnlyFileOpener {
	return &ReadOnlyFileOpener{
		fs:     &SystemFileOpener{},
		memory: NewBufferFileOpener(),
	}
}

func (o *ReadOnlyFileOpener) Open(filename string) (io.ReadWriteCloser, error) {
	return o.fs.Open(filename)
}

func (o *ReadOnlyFileOpener) Create(filename string) (io.ReadWriteCloser, error) {
	return o.memory.Create(filename)
}

func (o *ReadOnlyFileOpener) Write(filename string, content []byte) error {
	return o.memory.Write(filename, content)
}

func (o *ReadOnlyFileOpener) MkdirAll(dirPath string, perm os.FileMode) error {
	return o.memory.MkdirAll(dirPath, perm)
}

func (o *ReadOnlyFileOpener) Content(filename string) []byte {
	return o.memory.Content(filename)
}

// BlockWriterSettings represents the configuration settings for a BlockWriter, including placeholder name, block name, and block pattern.
type BlockWriterSettings struct {
	// Placeholder name describes the name of the block
	PlaceholderName string

	// BlockName describes optional the name of the block that the content will be written into.
	// When empty the content will be written directly into the file.
	BlockName string

	// BlockPattern describes the pattern that will be used to find the block in the file.
	BlockPattern string
}

// BlockWriterOption is a functional option type for configuring the BlockWriter, allowing for flexible and composable
type BlockWriterOption func(*BlockWriterSettings)

func WithPlaceholderName(name string) BlockWriterOption {
	return func(s *BlockWriterSettings) {
		s.PlaceholderName = name
	}
}

// blockWriter is responsible for writing content to a file while preserving existing blocks defined by special comments.
type blockWriter struct {
	fileName        string
	buf             bytes.Buffer
	block           string
	op              FileOpener
	blockName       string
	headerWritten   bool
	placeholderName string
	reBlocks        *regexp.Regexp
	reFragment      *regexp.Regexp
}

// nolint: revive //Why: it is perfectly
func NewBlockWriterWithOpener(fileName string, op FileOpener, opts ...BlockWriterOption) *blockWriter {
	settings := &BlockWriterSettings{
		PlaceholderName: "plumber",
	}
	for _, opt := range opts {
		opt(settings)
	}

	w := &blockWriter{
		fileName:        fileName,
		block:           settings.BlockPattern,
		op:              op,
		placeholderName: settings.PlaceholderName,
		blockName:       settings.BlockName,
		reBlocks: regexp.MustCompile(
			`(//|#) ?<<` + settings.PlaceholderName + `::Block\(([^)]+)\)>>` +
				`((?:\n|.)*?)(?://|#) ?<</` + settings.PlaceholderName + `::Block>>`),
		reFragment: regexp.MustCompile(
			`(//|#) ?<<` + settings.PlaceholderName + `::Fragment\(([^)]+)\)>>` +
				`((?:\n|.)*?)(?://|#)? ?<</` + settings.PlaceholderName + `::Fragment>>`),
	}
	return w
}

// nolint: revive //Why: it is just very normal
func NewBlockWriter(fileName string, opts ...BlockWriterOption) *blockWriter {
	return NewBlockWriterWithOpener(fileName, &SystemFileOpener{}, opts...)
}

func (w *blockWriter) Write(p []byte) (n int, err error) {
	if !w.headerWritten {
		w.headerWritten = true
		if w.block != "" {
			n, err = w.buf.Write([]byte("///" + w.blockName + "(" + w.block + ")\n"))
			if err != nil {
				return 0, fmt.Errorf("Was not able to write to a buffer: %w", err)
			}
		}
	}
	wn, err := w.buf.Write(p)
	n += wn
	return n, err
}

func (w *blockWriter) parseBlocks(body []byte) *Elements {
	parse := func(re *regexp.Regexp, body []byte) []Element {
		allIndexes := re.FindAllSubmatch(body, -1)
		elements := []Element{}
		for _, match := range allIndexes {
			name, params := w.parseParams(match[2])
			elements = append(elements, Element{
				Header:        match[2],
				Name:          name,
				CommentPrefix: match[1],
				Content:       match[3],
				Params:        params,
			})
		}
		return elements
	}

	return &Elements{
		Blocks:    parse(w.reBlocks, body),
		Fragments: parse(w.reFragment, body),
	}
}

// FragmentParams represents a collection of parameters for a fragment, allowing for easy retrieval and
// string representation of the parameters associated with a fragment in the generated content.
type FragmentParams []FragmentParam

func (fp FragmentParams) ByName(name string) (FragmentParam, bool) {
	for _, p := range fp {
		if p.Name == name {
			return p, true
		}
	}
	return FragmentParam{}, false
}

func (fp FragmentParams) String(prefixes ...string) string {
	fragments := []string{}
	for _, p := range fp {
		v := p.Name
		if p.Value != "" {
			v += "=" + p.Value
		}
		fragments = append(fragments, v)
	}
	return strings.Join(prefixes, "") + strings.Join(fragments, " ")
}

// FragmentParam represents a single parameter for a fragment, containing a name and an optional value.
type FragmentParam struct {
	Name  string
	Value string
}

func (w *blockWriter) parseParams(b []byte) ([]byte, FragmentParams) {
	params := FragmentParams{}
	parts := bytes.SplitN(b, []byte(" "), 2)
	if len(parts) == 1 {
		return parts[0], params
	}
	matches := reParams.FindAllStringSubmatch(string(parts[1]), -1)
	for _, m := range matches {
		params = append(params, FragmentParam{Name: m[1], Value: m[2]})
	}
	return parts[0], params
}

func (w *blockWriter) Close() error {
	if w.block != "" {
		if _, err := w.Write([]byte("\n///End" + w.blockName + "(" + w.block + ")")); err != nil {
			return fmt.Errorf("Was not able write: %w", err)
		}
	}

	var (
		blocks *Elements
		body   []byte
	)

	f, err := w.op.Open(w.fileName)
	if err == nil {
		body, err = io.ReadAll(f)
		if err != nil {
			return fmt.Errorf("Was not able read file contant: %w", err)
		}

		// Process bootstrap blocks only when we don't write to specialized block
		if w.block == "" {
			blocks = w.parseBlocks(body)
		}

		if err = f.Close(); err != nil {
			return fmt.Errorf("Was not able to close file: %w", err)
		}
	} else if !os.IsNotExist(err) || w.block != "" {
		return fmt.Errorf("Was not able to open file: %w", err)
	}

	f, err = w.op.Create(w.fileName)
	if err != nil {
		return fmt.Errorf("Was not able to overwrite file: %w", err)
	}

	if w.block == "" {
		body = w.buf.Bytes()
	} else {
		panic("not used")
		// Let's not support blocks for now
		// re := regexp.MustCompile(`\/{3} ?` + w.blockName + `\(` + w.block + `\)((.|\n)*?)\/{3} ?End` + w.blockName + `\(` + w.block + `\)`)
		// matches := re.FindAllSubmatch(body, 1)
		// if len(matches) > 0 {
		// 	//blocks = w.parseBlocks(matches[0][1])
		// }
		// body = re.ReplaceAll(body, w.buf.Bytes())
	}

	var buf bytes.Buffer
	// Restore blocks
	if blocks != nil {
		for _, b := range blocks.Blocks {
			name := string(b.Header)
			re := regexp.MustCompile(
				`(?://|#) ?<<` + w.placeholderName + `::Block\(` + name + `\)>>` +
					`((.|\n)*?)(?://|#) ?<</` + w.placeholderName + `::Block>>`)

			buf.WriteString(string(b.CommentPrefix) + ` <<` + w.placeholderName + `::Block(` + name + `)>>`)
			buf.Write(b.Content)
			buf.WriteString(string(b.CommentPrefix) + ` <</` + w.placeholderName + `::Block>>`)

			body = re.ReplaceAllLiteral(body, buf.Bytes())
			buf.Reset()
		}
	}

	if blocks != nil {
		for _, b := range blocks.Fragments {
			if _, found := b.Params.ByName("locked"); !found {
				continue
			}
			re := regexp.MustCompile(
				`(?://|#) ?<<` + w.placeholderName + `::Fragment\(` + string(b.Name) +
					`[^\)]*\)>>((.|\n)*?)(?://|#) ?<</` + w.placeholderName + `::Fragment>>`)

			buf.WriteString(string(b.CommentPrefix) + ` <<` + w.placeholderName + `::Fragment(` + string(b.Name) + b.Params.String(" ") + `)>>`)
			buf.Write(b.Content)
			buf.WriteString(string(b.CommentPrefix) + ` <</` + w.placeholderName + `::Fragment>>`)

			body = re.ReplaceAllLiteral(body, buf.Bytes())
			buf.Reset()
		}
	}

	_, err = f.Write(body)
	if err != nil {
		return fmt.Errorf("Was not write to file: %w", err)
	}

	defer f.Close()

	return nil
}

func FindBlocks(blockName string, body []byte) []BlockContent {
	blocks := []BlockContent{}
	re := regexp.MustCompile(`\/{3} ?` + blockName + `\(([^\)]+)\)((.|\n)*?)\/{3} ?End` + blockName + `\(([^\)]+)\)`)
	for _, match := range re.FindAllSubmatch(body, -1) {
		blocks = append(blocks, BlockContent{
			Name:    match[1],
			Content: match[2],
		})
	}
	return blocks
}

// Writer provides functionality to write generated content to files, with support for byte processing,
// block handling, and file opening through configurable options.
type Writer struct {
	config WriterConfig
}

// WriterConfig is a configuration struct for the Writer, containing settings for byte processors, output directory,
// overwrite behavior, block handling, and file opening.
type WriterConfig struct {
	ByteProcessors []ByteProcessor
	OutputDir      string
	Overwrite      bool
	Block          string
	BlockName      string
	PostProcessors PostProcessors
	WriterOptions  []BlockWriterOption
	FileOpener     FileOpener
}

func WithFileOpener(op FileOpener) WriterOption {
	return func(opts *WriterConfig) {
		opts.FileOpener = op
	}
}

func WithByteProcessors(byteProcessors []ByteProcessor) WriterOption {
	return func(opts *WriterConfig) {
		opts.ByteProcessors = byteProcessors
	}
}

func (c *WriterConfig) Clone() WriterConfig {
	return WriterConfig{
		WriterOptions:  c.WriterOptions,
		OutputDir:      c.OutputDir,
		Overwrite:      c.Overwrite,
		Block:          c.Block,
		BlockName:      c.BlockName,
		ByteProcessors: c.ByteProcessors,
		PostProcessors: c.PostProcessors,
		FileOpener:     c.FileOpener,
	}
}

// nolint: gocritic //Why: We want maintain data immutability and avoid side effects when applying options
func (c WriterConfig) Apply(opts ...WriterOption) WriterConfig {
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

func (c *WriterConfig) OutputFilePath(fileName string) string {
	return path.Join(c.OutputDir, fileName)
}

// WriterOption is a functional option type for configuring the Writer, allowing for flexible and composable
// configuration of the Writer's behavior and settings when writing generated content to files.
type WriterOption func(*WriterConfig)

func NewWriter(opts ...WriterOption) *Writer {
	w := &Writer{
		config: WriterConfig{
			FileOpener: &SystemFileOpener{},
		},
	}
	for _, opt := range opts {
		opt(&w.config)
	}
	return w
}

func (w *Writer) Write(
	ctx *Context,
	fileName string,
	openFileFunc func(io.Writer) error,
	opts ...WriterOption,
) error {
	cfg := w.config.Clone().Apply(opts...)

	fileName = cfg.OutputFilePath(fileName)

	if !cfg.Overwrite {
		if _, err := os.Stat(fileName); err == nil {
			return nil
		}
	}

	err := cfg.FileOpener.MkdirAll(path.Dir(fileName), os.ModePerm)
	if err != nil {
		return fmt.Errorf("can't create dir %s: %w", path.Dir(fileName), err)
	}

	var f = NewBlockWriterWithOpener(fileName, cfg.FileOpener, cfg.WriterOptions...)

	writer := NewByteProcessingWriter(fileName, cfg.ByteProcessors, f)

	if err := openFileFunc(writer); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	if err := cfg.PostProcessors.Apply(fileName); err != nil {
		return err
	}

	return nil
}
