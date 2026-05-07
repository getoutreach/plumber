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

	"github.com/charmbracelet/bubbles/viewport"
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
	ColorActive = lipgloss.Color("#757b78")
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
		return "CONSIDERED"
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
	// committed indicates the panel has been printed to the terminal
	// scrollback (via tea.Println) and must no longer be included in the
	// live View() output. Once a panel is committed it is effectively
	// frozen — no further updates should be applied to it.
	committed bool
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
// transition into review mode so the user can scroll through the full
// transcript before exiting.
type quitMsg struct{}

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

// phase represents the lifecycle stage of the TUI program. The TUI starts in
// phaseLive while transformers are still running and transitions to
// phaseReview once Wait() is called so the user can scroll through the full
// transcript inside an alternate-screen viewport.
type phase int

// constants for the TUI phases. The TUI starts in phaseLive and transitions to
const (
	// phaseLive is the streaming phase: the TUI runs inline (no alt screen),
	// completed panels are committed to the terminal scrollback via
	// tea.Println, and only the active (still-uncommitted) panel is shown in
	// the live area.
	phaseLive phase = iota
	// phaseReview is the post-run review phase: the TUI takes over the
	// alternate screen and presents the full transcript inside a scrollable
	// viewport. The user exits with q or ctrl+c.
	phaseReview
)

// tuiModel implements tea.Model. It owns the ordered list of panels and the
// index lookup used to update an existing panel when a follow-up event
// arrives. After all transformers have completed, the model transitions into
// review mode and renders the full transcript through the embedded viewport.
type tuiModel struct {
	panels   []*panel
	byKey    map[contract.Transformer]int
	finished bool
	width    int
	height   int
	phase    phase
	viewport viewport.Model
}

// newModel constructs an empty model with initialised lookup maps.
func newModel() *tuiModel {
	return &tuiModel{
		byKey:  make(map[contract.Transformer]int),
		width:  80,
		height: 24,
		phase:  phaseLive,
	}
}

// Init satisfies tea.Model. The TUI has no startup command.
func (m *tuiModel) Init() tea.Cmd {
	return nil
}

// Update processes incoming bubbletea messages, mutating the panel list in
// response to reporter events while in the live phase, and delegating
// scrolling key handling to the embedded viewport while in the review phase.
func (m *tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		if m.phase == phaseReview {
			m.viewport.Width = msg.Width
			m.viewport.Height = m.reviewBodyHeight()
			m.viewport.SetContent(m.renderTranscript())
		}
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			// In live phase, ctrl+c/q aborts the run early and behaves like a
			// completion signal: commit any panels still in the live area and
			// switch to review mode so the user can still scroll through the
			// transcript before exiting. In review phase, the same key
			// terminates the program.
			if m.phase == phaseReview {
				return m, tea.Quit
			}
			m.finished = true
			return m, tea.Sequence(m.flushAll(), m.enterReviewCmd())
		}
		// In review phase forward all other keys to the viewport so its
		// default key map (up/down, pgup/pgdn, home/end) takes effect.
		if m.phase == phaseReview {
			var cmd tea.Cmd
			m.viewport, cmd = m.viewport.Update(msg)
			return m, cmd
		}
	case tea.MouseMsg:
		if m.phase == phaseReview {
			var cmd tea.Cmd
			m.viewport, cmd = m.viewport.Update(msg)
			return m, cmd
		}
	case eventMsg:
		if m.phase == phaseReview {
			// Late-arriving events after the user has begun reviewing are
			// dropped — the transcript is considered immutable in review.
			return m, nil
		}
		c := m.applyEvent(msg.event)
		return m, c
	case quitMsg:
		m.finished = true
		// Flush any uncommitted panels first, then transition to review so
		// scrollback contains the full record while the alt-screen viewport
		// also presents it.
		return m, tea.Sequence(m.flushAll(), m.enterReviewCmd())
	}
	return m, nil
}

// commitPanel marks the panel as committed and returns a command that prints
// it to the terminal scrollback above the live area. Committed panels are
// excluded from subsequent View() output so the same content is never
// rendered twice.
func (m *tuiModel) commitPanel(p *panel) tea.Cmd {
	if p == nil || p.committed {
		return nil
	}
	p.committed = true
	return tea.Println(renderPanel(p, m.width))
}

// flushAll commits every panel that has not yet been printed to scrollback,
// preserving the original panel order. Used at shutdown so no transformer
// blocks are lost when bubbletea tears down its live area.
func (m *tuiModel) flushAll() tea.Cmd {
	var cmds []tea.Cmd
	for _, p := range m.panels {
		if cmd := m.commitPanel(p); cmd != nil {
			cmds = append(cmds, cmd)
		}
	}
	if len(cmds) == 0 {
		return nil
	}
	return tea.Sequence(cmds...)
}

// applyEvent mutates the model to reflect a single reporter event, creating a
// new panel for the transformer if one does not yet exist and otherwise
// updating the existing panel's status or appending log lines as appropriate.
// Panels that reach a terminal status (success, error, skipped) are committed
// to the terminal scrollback so they are not lost when the live area shrinks
// or the program exits; the returned command carries any required tea.Println
// invocations.
// nolint: cyclop,funlen //Why: tui
func (m *tuiModel) applyEvent(e contract.ReporterEvent) tea.Cmd {
	switch e.Kind {
	case contract.EventTransformerAdded:
		m.ensurePanel(e)
	case contract.EventTransformerSkipped:
		p := m.ensurePanel(e)
		p.status = statusSkipped
		if e.Message != "" {
			p.logs = append(p.logs, "skipped: "+e.Message)
		}
		return m.commitPanel(p)
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
		return m.commitPanel(p)
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
		if p.status == statusSuccess {
			return m.commitPanel(p)
		}
	case contract.EventTransformerRestored:
		// Restored events are not bound to a specific transformer. Render
		// each restored event as its own headerless panel whose body is a
		// pair of key/value rows styled identically to annotation rows.
		// Restored panels are inherently terminal and have no further
		// updates, so commit them to scrollback immediately.
		annotations := []annotationKV{
			{Name: "restored", Value: e.Path},
		}
		if e.Error != nil {
			annotations = append(annotations, annotationKV{Name: "error", Value: e.Error.Error()})
		}
		p := &panel{
			headerless:  true,
			annotations: annotations,
		}
		m.panels = append(m.panels, p)
		return m.commitPanel(p)
	case contract.EventQueryExecuted:
		if len(m.panels) == 0 {
			return nil
		}
		p := m.lastLivePanel()
		if p == nil {
			return nil
		}
		p.logs = append(p.logs, "query: "+e.Message)
	case contract.EventQueryError:
		if len(m.panels) == 0 {
			return nil
		}
		p := m.lastLivePanel()
		if p == nil {
			return nil
		}
		if e.Error != nil {
			p.logs = append(p.logs, "query error: "+e.Error.Error())
		}
	}
	return nil
}

// lastLivePanel returns the most recently appended panel that has not yet
// been committed to scrollback. Query events attach to the panel currently
// being shown in the live area; once a panel is frozen it must not be
// mutated, otherwise the committed copy in scrollback would diverge from
// the in-memory state.
func (m *tuiModel) lastLivePanel() *panel {
	for i := len(m.panels) - 1; i >= 0; i-- {
		if !m.panels[i].committed {
			return m.panels[i]
		}
	}
	return nil
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

// View renders the current frame for either the live phase or the review
// phase. During the live phase only the still-uncommitted panels are shown
// (committed panels live in the terminal scrollback). During the review
// phase the embedded viewport renders the full transcript with a footer
// describing the available scroll keys.
func (m *tuiModel) View() string {
	if m.phase == phaseReview {
		return m.viewport.View() + "\n" + m.reviewFooter()
	}
	live := make([]*panel, 0, len(m.panels))
	for _, p := range m.panels {
		if !p.committed {
			live = append(live, p)
		}
	}
	if len(live) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("\n")
	for i, p := range live {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(renderPanel(p, m.width))
		b.WriteString("\n")
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Review mode
// ---------------------------------------------------------------------------

// reviewFooter is the always-visible status bar shown beneath the review
// viewport. It documents the active scroll keys and the exit shortcut so the
// user knows how to navigate the transcript.
func (m *tuiModel) reviewFooter() string {
	pct := 0
	if m.viewport.TotalLineCount() > 0 {
		pct = int(m.viewport.ScrollPercent() * 100)
	}
	hint := fmt.Sprintf(" review — ↑/↓ pgup/pgdn home/end scroll · q to exit · %3d%% ", pct)
	return reviewFooterStyle.Width(m.width).Render(hint)
}

// reviewBodyHeight is the vertical space (in lines) available for the
// viewport body, leaving exactly one line at the bottom for the footer. A
// minimum of one line is always returned so the viewport never collapses to
// zero height.
func (m *tuiModel) reviewBodyHeight() int {
	h := m.height - 1
	if h < 1 {
		h = 1
	}
	return h
}

// renderTranscript builds the full multi-panel transcript shown by the
// review viewport. Every panel — including those already committed to
// scrollback during the live phase — is rendered so the user can scroll
// through the entire record without leaving the program.
func (m *tuiModel) renderTranscript() string {
	if len(m.panels) == 0 {
		return ""
	}
	var b strings.Builder
	for i, p := range m.panels {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(renderPanel(p, m.width))
		b.WriteString("\n")
	}
	return b.String()
}

// enterReviewCmd transitions the program from the live phase into the
// review phase: it switches the bubbletea program into the alternate screen
// buffer (so the live transcript and the user's prior shell session remain
// untouched), seeds the viewport with the rendered transcript, and primes
// the viewport dimensions from the most recent window-size observation.
//
// When there are no panels to review (e.g. the run produced no transformer
// events), the command short-circuits to tea.Quit so the user is not left
// staring at an empty review screen.
//
// The returned tea.Cmd is meant to be appended to the end of any commit
// sequence so the alt-screen switch happens after all panels have been
// committed to scrollback.
func (m *tuiModel) enterReviewCmd() tea.Cmd {
	if len(m.panels) == 0 {
		return tea.Quit
	}
	return tea.Sequence(
		func() tea.Msg {
			m.phase = phaseReview
			m.viewport = viewport.New(m.width, m.reviewBodyHeight())
			m.viewport.SetContent(m.renderTranscript())
			// Force a follow-up window-size observation so the viewport
			// re-syncs against the real terminal dimensions after the
			// alternate screen takes effect. Returning nil keeps the
			// sequence flowing to tea.EnterAltScreen which will itself
			// trigger a fresh tea.WindowSizeMsg.
			return nil
		},
		tea.EnterAltScreen,
	)
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

// reviewFooterStyle styles the always-visible footer shown beneath the
// review viewport. It uses the panel border colour as its background so the
// footer reads as a clearly distinct status bar separated from the panel
// content above it.
var reviewFooterStyle = lipgloss.NewStyle().
	Background(ColorBorder).
	Foreground(ColorHeaderText).
	Bold(true)

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

// NewReporter creates and starts a new TUI reporter. The program initially
// runs in inline mode (rather than the alternate screen buffer) so that
// completed transformer panels stream into the terminal scrollback as they
// finish. Once Wait() is called the program transitions into a full-screen
// review phase backed by a scrollable viewport so the user can scroll
// through the entire transcript before pressing q to exit. Mouse-wheel
// scrolling is enabled in review mode where the host terminal supports it.
//
// The bubbletea program runs in a separate goroutine; call Wait() after all
// transformations complete to enter review mode and block until the user
// dismisses it.
func NewReporter() *Reporter {
	m := newModel()
	prog := tea.NewProgram(m, tea.WithMouseCellMotion())
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
