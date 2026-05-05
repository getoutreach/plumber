// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements a bubbletea-based TUI reporter for the shape command,
// rendering transformer progress as a fullscreen panel with live log output.

// Package tui provides a bubbletea-based interactive reporter for the shape command.
// Each transformer is rendered as its own panel composed of a colored status header,
// the transformer's fully qualified name, its annotations, and a streaming log area.
// Panels are rendered one after another in the order in which they are received and
// are updated in place when subsequent events (skip, error, info, output) arrive for
// the same transformer.
package tui

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/query/model"
)

// ---------------------------------------------------------------------------
// Colors — all colors are explicitly declared here for easy customization.
// ---------------------------------------------------------------------------
var (
	// ColorActive is the header background for the currently processing transformer.
	ColorActive = lipgloss.Color("#27AE60")
	// ColorSuccess is the header background for a successfully completed transformer.
	ColorSuccess = lipgloss.Color("#2ECC71")
	// ColorError is the header background for a transformer that encountered an error.
	ColorError = lipgloss.Color("#E74C3C")
	// ColorSkipped is the header background for a transformer that was skipped.
	ColorSkipped = lipgloss.Color("#F39C12")
	// ColorPanelBg is the background color for panel bodies. It is intentionally
	// kept a few shades lighter than typical dark terminals so the panel body
	// is clearly distinguishable from the surrounding terminal background.
	ColorPanelBg = lipgloss.Color("#3C4250")
	// ColorAnnotationKey is the color for annotation names.
	ColorAnnotationKey = lipgloss.Color("#7F8C8D")
	// ColorAnnotationValue is the color for annotation values. Kept noticeably
	// lighter than the panel body background so the values pop against the
	// panel and remain easy to read at a glance.
	ColorAnnotationValue = lipgloss.Color("#ECF0F1")
	// ColorLogText is the color for log text in the log area.
	ColorLogText = lipgloss.Color("#ABB2BF")
	// ColorHeaderText is the color for text in the header bar.
	ColorHeaderText = lipgloss.Color("#FFFFFF")
	// ColorBorder is the color for panel borders (the leading vertical bar).
	ColorBorder = lipgloss.Color("#555555")
)

// barGlyph is the character used as the leading vertical bar on every line of a panel.
const barGlyph = "▋"

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

// status represents the lifecycle state of a transformer panel.
type status int

// The status of a transformer starts as active when the transformer is first
const (
	// statusActive indicates the transformer is currently processing. The header
	statusActive status = iota
	// statusSuccess indicates the transformer completed successfully.
	statusSuccess
	// statusError indicates the transformer encountered an error.
	statusError
	// statusSkipped indicates the transformer was skipped, either due to an explicit skip event or because it
	// was never added in the first place.
	statusSkipped
)

// String returns a short, upper-case textual representation of the status,
// suitable for rendering in the panel header.
func (s status) String() string {
	switch s {
	case statusActive:
		return "OK"
	case statusSuccess:
		return "SUCCESS"
	case statusError:
		return "ERROR"
	case statusSkipped:
		return "SKIPPED"
	default:
		return "UNKNOWN"
	}
}

// color returns the background color associated with the status.
func (s status) color() lipgloss.Color {
	switch s {
	case statusActive:
		return ColorActive
	case statusSuccess:
		return ColorSuccess
	case statusError:
		return ColorError
	case statusSkipped:
		return ColorSkipped
	default:
		return ColorBorder
	}
}

// ---------------------------------------------------------------------------
// Panel
// ---------------------------------------------------------------------------

// panel is the in-memory representation of a single transformer block rendered
// by the TUI. The same panel is updated in place as further events for the
// transformer are received.
//
// A panel may also be "headerless" — used for synthetic blocks such as
// restored-output panels — in which case only the key/value rows from the
// annotations slice are rendered with no colored status header.
type panel struct {
	transformer contract.Transformer
	name        string
	fqn         string
	status      status
	annotations []annotationKV
	logs        []string
	// headerless indicates the panel must be rendered without the colored
	// status header line. Used for restored-output blocks which do not belong
	// to any single transformer.
	headerless bool
}

// annotationKV is a flat key/value representation of an annotation used by the
// renderer. Multi-valued annotations are collapsed into a single string so the
// renderer remains a straightforward two-column print.
type annotationKV struct {
	Name  string
	Value string
}

// fqnFromNode extracts the fully-qualified name from a model.Node when the
// node is a *model.Type. Other node kinds do not carry an FQN and yield "".
func fqnFromNode(node model.Node) string {
	if node == nil {
		return ""
	}
	if t, ok := node.(*model.Type); ok && t != nil {
		return t.Spec.FQN
	}
	return fmt.Sprintf("%T", node)
}

// annotationKVs collapses a model.Annotations slice into the simpler key/value
// pairs used by the renderer. Positional args and named args are joined with
// commas so each annotation occupies a single line.
func annotationKVs(anns model.Annotations) []annotationKV {
	out := make([]annotationKV, 0, len(anns))
	for _, a := range anns {
		out = append(out, annotationKV{Name: a.Name, Value: formatAnnotationValue(a)})
	}
	return out
}

// formatAnnotationValue renders an annotation's positional and named arguments
// into a single human-readable string.
func formatAnnotationValue(a model.Annotation) string {
	parts := make([]string, 0, len(a.Args)+len(a.NamedArgs))
	parts = append(parts, a.Args...)
	if len(a.NamedArgs) > 0 {
		keys := make([]string, 0, len(a.NamedArgs))
		for k := range a.NamedArgs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s=%s", k, a.NamedArgs[k]))
		}
	}
	return strings.Join(parts, ", ")
}

// ---------------------------------------------------------------------------
// Bubbletea messages
// ---------------------------------------------------------------------------

// eventMsg wraps a contract.ReporterEvent so it can travel through the
// bubbletea program's message loop.
type eventMsg struct {
	event contract.ReporterEvent
}

// quitMsg signals that no further events will be sent and the program should
// terminate after rendering its final frame.
type quitMsg struct{}

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

// tuiModel implements tea.Model. It owns the ordered list of panels and the
// index lookup used to update an existing panel when a follow-up event arrives.
type tuiModel struct {
	panels   []*panel
	byKey    map[contract.Transformer]int
	finished bool
	width    int
}

// newModel constructs an empty model with initialised lookup maps.
func newModel() *tuiModel {
	return &tuiModel{
		byKey: make(map[contract.Transformer]int),
		width: 80,
	}
}

// Init satisfies tea.Model. The TUI has no startup command.
func (m *tuiModel) Init() tea.Cmd {
	return nil
}

// Update processes incoming bubbletea messages, mutating the panel list in
// response to reporter events and quitting when signalled.
func (m *tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		return m, nil
	case tea.KeyMsg:
		// Allow the user to abort early with Ctrl+C / q.
		switch msg.String() {
		case "ctrl+c", "q":
			m.finished = true
			return m, tea.Quit
		}
	case eventMsg:
		m.applyEvent(msg.event)
		return m, nil
	case quitMsg:
		m.finished = true
		return m, tea.Quit
	}
	return m, nil
}

// applyEvent mutates the model to reflect a single reporter event, creating a
// new panel for the transformer if one does not yet exist and otherwise
// updating the existing panel's status or appending log lines as appropriate.
func (m *tuiModel) applyEvent(e contract.ReporterEvent) {
	switch e.Kind {
	case contract.EventTransformerAdded:
		m.ensurePanel(e)
	case contract.EventTransformerSkipped:
		p := m.ensurePanel(e)
		p.status = statusSkipped
		if e.Message != "" {
			p.logs = append(p.logs, "skipped: "+e.Message)
		}
	case contract.EventTransformerError:
		p := m.ensurePanel(e)
		p.status = statusError
		if e.Error != nil {
			var syntaxErr *contract.SyntaxError
			if errors.As(e.Error, &syntaxErr) {
				p.logs = append(p.logs, "error: "+syntaxErr.String())
			} else {
				p.logs = append(p.logs, "error: "+e.Error.Error())
			}
		}
	case contract.EventTransformerInfo:
		p := m.ensurePanel(e)
		if e.Message != "" {
			p.logs = append(p.logs, e.Message)
		}
	case contract.EventTransformerOutput:
		p := m.ensurePanel(e)
		if p.status == statusActive {
			p.status = statusSuccess
		}
		if e.Path != "" {
			p.logs = append(p.logs, "output: "+e.Path)
		}
	case contract.EventTransformerRestored:
		// Restored events are not bound to a specific transformer. Render
		// each restored event as its own headerless panel whose body is a
		// pair of key/value rows styled identically to annotation rows.
		annotations := []annotationKV{
			{Name: "restored", Value: e.Path},
		}
		if e.Error != nil {
			annotations = append(annotations, annotationKV{Name: "error", Value: e.Error.Error()})
		}
		m.panels = append(m.panels, &panel{
			headerless:  true,
			annotations: annotations,
		})
	case contract.EventQueryExecuted:
		if len(m.panels) == 0 {
			return
		}
		p := m.panels[len(m.panels)-1]
		p.logs = append(p.logs, "query: "+e.Message)
	case contract.EventQueryError:
		if len(m.panels) == 0 {
			return
		}
		p := m.panels[len(m.panels)-1]
		if e.Error != nil {
			p.logs = append(p.logs, "query error: "+e.Error.Error())
		}
	}
}

// ensurePanel returns the panel associated with the event's transformer,
// creating and appending a new one when first encountered.
func (m *tuiModel) ensurePanel(e contract.ReporterEvent) *panel {
	if e.Transformer == nil {
		// Synthesise a placeholder panel so events without a transformer still
		// have somewhere to render their logs.
		p := &panel{name: "(no transformer)", status: statusActive}
		m.panels = append(m.panels, p)
		return p
	}
	if idx, ok := m.byKey[e.Transformer]; ok {
		return m.panels[idx]
	}
	p := &panel{
		transformer: e.Transformer,
		name:        e.Transformer.GetName(),
		fqn:         fqnFromNode(e.Node),
		status:      statusActive,
		annotations: annotationKVs(e.Transformer.GetAnnotations()),
	}
	m.byKey[e.Transformer] = len(m.panels)
	m.panels = append(m.panels, p)
	return p
}

// View renders all panels stacked vertically in the order they were created.
// Each panel is expanded to the full terminal width tracked from the most
// recent tea.WindowSizeMsg so panels and headers visually fill the screen.
// A leading blank line is emitted before the first panel so the output is
// visually separated from any preceding terminal content (prompt, command,
// etc.).
func (m *tuiModel) View() string {
	if len(m.panels) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("\n")
	for i, p := range m.panels {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(renderPanel(p, m.width))
		b.WriteString("\n")
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------

// barStyle styles the leading vertical bar prefix of every panel line. The
// bar shares the panel body background so the prefix blends seamlessly into
// the panel rather than appearing as a stripe of terminal background between
// the bar glyph and the body content.
var barStyle = lipgloss.NewStyle().
	Foreground(ColorBorder).
	Background(ColorPanelBg)

// headerStyle styles the colored header text inside a panel. The header keeps
// its own status-specific background and is therefore not subject to the panel
// body background.
func headerStyle(s status) lipgloss.Style {
	return lipgloss.NewStyle().
		Background(s.color()).
		Foreground(ColorHeaderText).
		Bold(true).
		Padding(0, 1)
}

// annotationKeyStyle styles annotation keys.
var annotationKeyStyle = lipgloss.NewStyle().
	Background(ColorPanelBg).
	Foreground(ColorAnnotationKey).
	Bold(true)

// annotationValueStyle styles annotation values.
var annotationValueStyle = lipgloss.NewStyle().
	Background(ColorPanelBg).
	Foreground(ColorAnnotationValue)

// logStyle styles a single log line.
var logStyle = lipgloss.NewStyle().
	Background(ColorPanelBg).
	Foreground(ColorLogText)

// panelLineStyle wraps the rendered body content of a panel line so that the
// background extends across the whole visible width of the line, giving the
// panel a clear, well-defined body.
var panelLineStyle = lipgloss.NewStyle().Background(ColorPanelBg)

// renderPanel produces the multi-line string representation of a single panel,
// with each line prefixed by the configured vertical bar glyph. Both the
// status header and the body lines are expanded to fill the full terminal
// width so the panel visually spans the entire screen.
//
// termWidth is the most recently observed terminal width (in cells). When the
// terminal width is unknown (zero), a sensible minimum is used so the panel
// remains legible before the first tea.WindowSizeMsg arrives.
func renderPanel(p *panel, termWidth int) string {
	const minPanelWidth = 60
	// The leading bar plus its trailing space consume two visible cells; the
	// remainder of the line is available for the panel content.
	const barPrefixWidth = 2

	if termWidth < minPanelWidth {
		termWidth = minPanelWidth
	}
	contentWidth := termWidth - barPrefixWidth
	if contentWidth < 1 {
		contentWidth = 1
	}

	type bodyLine struct {
		text       string
		visibleLen int
	}

	var lines []bodyLine
	addBody := func(rendered, plain string) {
		lines = append(lines, bodyLine{text: rendered, visibleLen: lipgloss.Width(plain)})
	}

	// renderKeyValue produces a key/value line styled identically to the
	// annotation rows; used for both the FQN line and the annotation list so
	// the panel body presents a consistent two-column layout.
	renderKeyValue := func(name, value string) (string, string) {
		plain := name + ": " + value
		rendered := annotationKeyStyle.Render(name) +
			panelLineStyle.Render(": ") +
			annotationValueStyle.Render(value)
		return rendered, plain
	}

	// Headerless panels (e.g. restored-output blocks) skip the colored
	// status header, the FQN line, and the trailing log area entirely; they
	// only render their key/value rows on the panel background.
	if !p.headerless {
		if p.fqn != "" {
			// One blank line of separation between the colored header and the
			// FQN line for visual breathing room.
			addBody("", "")
			rendered, plain := renderKeyValue("FQN", p.fqn)
			addBody(rendered, plain)
		}

		if len(p.annotations) > 0 {
			addBody("", "")
			for _, a := range p.annotations {
				rendered, plain := renderKeyValue(a.Name, a.Value)
				addBody(rendered, plain)
			}
		}

		// Always render the separator and log area so the panel layout remains
		// visually consistent even before any logs are appended.
		addBody("", "")
		if len(p.logs) == 0 {
			addBody(logStyle.Render(""), "")
		} else {
			for _, log := range p.logs {
				// Each log line may itself contain newlines (e.g. multi-line errors);
				// split so the bar prefix is applied to every visual line.
				for _, sub := range strings.Split(log, "\n") {
					addBody(logStyle.Render(sub), sub)
				}
			}
		}
	} else {
		// Headerless panels render their annotations directly with no
		// surrounding separators.
		for _, a := range p.annotations {
			rendered, plain := renderKeyValue(a.Name, a.Value)
			addBody(rendered, plain)
		}
	}

	// The body bar prefix renders the glyph plus a trailing space using the
	// panel background, so the join between the bar and the panel body is
	// seamless on every body line.
	bodyBar := barStyle.Render(barGlyph + " ")

	// padBody pads a body line to the full content width so the panel
	// background extends across the entire terminal width.
	padBody := func(l bodyLine) string {
		pad := contentWidth - l.visibleLen
		if pad < 0 {
			pad = 0
		}
		return l.text + panelLineStyle.Render(strings.Repeat(" ", pad))
	}

	if p.headerless {
		// Headerless panels emit only their body rows.
		prefixed := make([]string, 0, len(lines))
		for _, l := range lines {
			prefixed = append(prefixed, bodyBar+padBody(l))
		}
		return strings.Join(prefixed, "\n")
	}

	// Header expands to the full content width with its status background.
	headerText := fmt.Sprintf("%s  %s", p.status.String(), p.name)
	header := headerStyle(p.status).Width(contentWidth).Render(headerText)

	// The header bar mirrors the body bar but uses the status background so
	// the bar prefix on the header row matches the colored header behind it
	// rather than peeking through with the panel body color.
	headerBar := lipgloss.NewStyle().
		Foreground(ColorBorder).
		Background(p.status.color()).
		Render(barGlyph + " ")

	// First line is always the header. Body lines are padded to contentWidth
	// so the panel background extends across the full terminal width.
	prefixed := make([]string, 0, len(lines)+1)
	prefixed = append(prefixed, headerBar+header)
	for _, l := range lines {
		prefixed = append(prefixed, bodyBar+padBody(l))
	}
	return strings.Join(prefixed, "\n")
}

// ---------------------------------------------------------------------------
// TUIReporter — implements contract.Reporter
// ---------------------------------------------------------------------------

// Reporter is a bubbletea-based reporter that renders transformer progress
// as a fullscreen panel with live log output. The bubbletea program runs in a
// dedicated goroutine; reporter callbacks marshal events onto the program's
// message queue so updates remain serialised inside the model's Update method.
type Reporter struct {
	program *tea.Program
	model   *tuiModel
	done    chan struct{}
	once    sync.Once
}

// NewReporter creates and starts a new TUI reporter. The program runs in
// inline mode (rather than the alternate screen buffer) so that the final
// rendered panels remain visible in the terminal scrollback after the program
// exits. The bubbletea program runs in a separate goroutine; call Wait()
// after all transformations complete to flush the final frame and shut down
// the renderer.
func NewReporter() *Reporter {
	m := newModel()
	prog := tea.NewProgram(m)
	r := &Reporter{
		program: prog,
		model:   m,
		done:    make(chan struct{}),
	}
	go func() {
		defer close(r.done)
		// The returned model and error are not surfaced: terminating early
		// (e.g. Ctrl+C) is treated the same as a normal shutdown so the caller
		// can continue with its own cleanup.
		_, err := prog.Run()
		if err != nil {
			fmt.Printf("TUI program exited with error: %v\n", err)
		}
	}()
	return r
}

// Notify sends a reporter event to the bubbletea program for rendering.
// The call is non-blocking from the perspective of the caller: bubbletea
// queues the message and processes it on its event loop.
func (r *Reporter) Notify(event contract.ReporterEvent) {
	if r == nil || r.program == nil {
		return
	}
	r.program.Send(eventMsg{event: event})
}

// Wait signals that all transformations are complete and waits for the
// bubbletea program to finish rendering and exit. It is safe to call Wait
// multiple times; only the first call sends the quit signal.
func (r *Reporter) Wait() {
	if r == nil || r.program == nil {
		return
	}
	r.once.Do(func() {
		r.program.Send(quitMsg{})
	})
	<-r.done
}
