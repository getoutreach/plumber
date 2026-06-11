// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the embedded skills filesystem and the public
// API for listing skills and installing them onto a coding-agent platform
// (agents, claude, copilot) with optional describe-driven template expansion.

// Package skills provides discovery and installation of plumber-authored
// "skills" — bundles of agent-facing markdown documentation — onto coding
// agent platforms such as Claude, GitHub Copilot, and the project-local
// agents/ folder. Skill source content is embedded at build time so the
// CLI can install skills without network access. External skill folders
// declared via git source configuration are also discovered and merged
// alongside the embedded catalog at runtime.
package skills

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// skillsFS embeds every file under the skills/ directory. Each top-level
// directory is one installable skill; the skill's name is the directory name.
// A SKILL.md file at the root of the skill is required and provides the
// human-readable description used by listing.
//
//go:embed all:skills
var skillsFS embed.FS

// embeddedSkillsRoot is the path within skillsFS where skills are located.
const embeddedSkillsRoot = "skills"

// embeddedOrigin labels skills that come from the embedded catalog.
const embeddedOrigin = "embedded"

// SkillInfo describes a single discovered skill (embedded or external).
type SkillInfo struct {
	// Name is the skill's identifier (directory basename or SKILL.md
	// frontmatter `name`).
	Name string `json:"name" yaml:"name"`
	// Description is taken from the SKILL.md YAML frontmatter if present.
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	// Files lists the relative file paths within the skill (sorted, SKILL.md first).
	Files []string `json:"files,omitempty" yaml:"files,omitempty"`
	// Origin is "embedded" for the built-in catalog, or the source identifier
	// (typically a git repository URL) for external skills.
	Origin string `json:"origin" yaml:"origin"`

	// src is the unexported reader bundle used by Install to access the skill's
	// files; it is populated by ListAvailableSkills.
	src skillSource `json:"-" yaml:"-"`
}

// skillSource bundles a filesystem and a directory name that together locate
// a single skill's files. fsys is rooted at the parent of the skill directory
// so files live at "<name>/...".
type skillSource struct {
	fsys   fs.FS
	name   string
	origin string
}

// ExternalSource declares an additional, on-disk source containing a single
// skill directory to be discovered alongside the embedded catalog. Each Dir
// must be an existing directory whose contents include SKILL.md.
type ExternalSource struct {
	// Dir is the absolute filesystem path to the skill directory.
	Dir string
	// Origin is an optional identifier for diagnostics (typically a git
	// repository URL). Defaults to Dir when empty.
	Origin string
}

// InstallOptions controls a skills install run.
type InstallOptions struct {
	// Platforms is the resolved list of concrete platforms to install into.
	// Use ResolvePlatforms to expand PlatformAutodetect.
	Platforms []Platform
	// Skills is the set of skill names to install. Empty means "all available skills".
	Skills []string
	// External lists additional on-disk skill directories to discover
	// alongside the embedded catalog. External skills override embedded
	// skills with the same name.
	External []ExternalSource
	// DestRoot is the destination root directory. Defaults to "." when empty.
	DestRoot string
	// Force overwrites existing destination files when true; otherwise existing
	// files are skipped.
	Force bool
	// DryRun reports actions without writing any files.
	DryRun bool
	// TemplateContext is used to expand describe-driven template helpers in
	// skill markdown files. The zero value disables describe injection
	// (templates that reference describe helpers will still parse but emit
	// empty sections).
	TemplateContext TemplateContext
}

// FileResult records the outcome of installing a single file.
type FileResult struct {
	Path    string
	Skipped bool
	Reason  string
}

// InstallResult records the outcome of installing one skill onto one platform.
type InstallResult struct {
	Platform Platform
	Skill    string
	// Destination is the directory (folder platforms) or file path
	// (single-file platforms) where content was written.
	Destination string
	Files       []FileResult
}

// ListSkills returns metadata for every embedded skill, sorted by name.
// It is a backward-compatible shorthand for ListAvailableSkills(nil).
func ListSkills() ([]SkillInfo, error) {
	return ListAvailableSkills(nil)
}

// ListAvailableSkills returns metadata for every embedded skill plus any
// external sources, sorted by name. When an external skill has the same name
// as an embedded skill the external one wins and a notice is written to
// stderr. Each ExternalSource's Dir must exist and contain a SKILL.md;
// directories that do not satisfy these requirements produce an error.
func ListAvailableSkills(external []ExternalSource) ([]SkillInfo, error) {
	embeddedRoot, err := fs.Sub(skillsFS, embeddedSkillsRoot)
	if err != nil {
		return nil, fmt.Errorf("rooting embedded skills: %w", err)
	}

	byName := map[string]SkillInfo{}

	embedded, err := discoverSkills(embeddedRoot, embeddedOrigin)
	if err != nil {
		return nil, err
	}
	for _, s := range embedded {
		byName[s.Name] = s
	}

	for _, ext := range external {
		s, err := loadExternalSkill(ext)
		if err != nil {
			return nil, err
		}
		if existing, ok := byName[s.Name]; ok && existing.Origin == embeddedOrigin {
			fmt.Fprintf(os.Stderr, "skill %q overridden by external source %s\n", s.Name, sourceLabel(ext))
		}
		byName[s.Name] = s
	}

	out := make([]SkillInfo, 0, len(byName))
	for _, s := range byName {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// discoverSkills walks the immediate children of root and returns one
// SkillInfo per subdirectory that contains a SKILL.md.
func discoverSkills(root fs.FS, origin string) ([]SkillInfo, error) {
	entries, err := fs.ReadDir(root, ".")
	if err != nil {
		return nil, fmt.Errorf("reading skills root: %w", err)
	}
	out := make([]SkillInfo, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		src := skillSource{fsys: root, name: e.Name(), origin: origin}
		info, err := loadSkillInfo(src)
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

// loadExternalSkill builds a SkillInfo for a single on-disk skill directory.
// The directory must exist and contain a SKILL.md.
func loadExternalSkill(ext ExternalSource) (SkillInfo, error) {
	if ext.Dir == "" {
		return SkillInfo{}, errors.New("external skill source: Dir is empty")
	}
	info, err := os.Stat(ext.Dir)
	if err != nil {
		return SkillInfo{}, fmt.Errorf("external skill %q: %w", ext.Dir, err)
	}
	if !info.IsDir() {
		return SkillInfo{}, fmt.Errorf("external skill %q is not a directory", ext.Dir)
	}
	if _, err := os.Stat(filepath.Join(ext.Dir, "SKILL.md")); err != nil {
		return SkillInfo{}, fmt.Errorf("external skill %q has no SKILL.md: %w", ext.Dir, err)
	}
	parent := filepath.Dir(ext.Dir)
	name := filepath.Base(ext.Dir)
	src := skillSource{
		fsys:   os.DirFS(parent),
		name:   name,
		origin: sourceLabel(ext),
	}
	skill, err := loadSkillInfo(src)
	if err != nil {
		return SkillInfo{}, err
	}
	return skill, nil
}

// sourceLabel returns the user-facing label for an ExternalSource (Origin
// when set, falling back to Dir).
func sourceLabel(ext ExternalSource) string {
	if ext.Origin != "" {
		return ext.Origin
	}
	return ext.Dir
}

// loadSkillInfo reads the files of a single skill from src and extracts its
// description from SKILL.md frontmatter when available. The skill's name
// defaults to the directory basename and is overridden by SKILL.md
// frontmatter `name` when present.
func loadSkillInfo(src skillSource) (SkillInfo, error) {
	files, err := listSkillFiles(src)
	if err != nil {
		return SkillInfo{}, err
	}
	info := SkillInfo{
		Name:   src.name,
		Files:  files,
		Origin: src.origin,
		src:    src,
	}

	skillMD := path(src.name, "SKILL.md")
	if data, err := fs.ReadFile(src.fsys, skillMD); err == nil {
		if fm := parseFrontmatter(data); fm != nil {
			if desc, ok := fm["description"].(string); ok {
				info.Description = desc
			}
			if n, ok := fm["name"].(string); ok && n != "" {
				info.Name = n
			}
		}
	}
	return info, nil
}

// listSkillFiles returns every file path inside a skill, relative to the
// skill directory. SKILL.md is sorted first; remaining files are alphabetical.
func listSkillFiles(src skillSource) ([]string, error) {
	root := src.name
	var files []string
	err := fs.WalkDir(src.fsys, root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking skill %q: %w", src.name, err)
	}
	sort.Slice(files, func(i, j int) bool {
		ai, aj := files[i] == "SKILL.md", files[j] == "SKILL.md"
		switch {
		case ai && !aj:
			return true
		case !ai && aj:
			return false
		default:
			return files[i] < files[j]
		}
	})
	return files, nil
}

// Install installs the requested skills onto every requested platform.
func Install(opts InstallOptions) ([]InstallResult, error) {
	if len(opts.Platforms) == 0 {
		return nil, errors.New("no platforms provided to Install")
	}
	destRoot := opts.DestRoot
	if destRoot == "" {
		destRoot = "."
	}

	available, err := ListAvailableSkills(opts.External)
	if err != nil {
		return nil, err
	}

	skills, err := selectSkills(available, opts.Skills)
	if err != nil {
		return nil, err
	}

	var results []InstallResult
	for _, platform := range opts.Platforms {
		for _, s := range skills {
			r, err := installOne(platform, s, destRoot, opts)
			if err != nil {
				return results, fmt.Errorf("installing %q to %q: %w", s.Name, platform, err)
			}
			results = append(results, r)
		}
	}
	return results, nil
}

// selectSkills returns the SkillInfo for each requested name, or all
// available skills when names is empty. Unknown names produce an error
// listing the available skills.
func selectSkills(all []SkillInfo, names []string) ([]SkillInfo, error) {
	if len(names) == 0 {
		return all, nil
	}
	index := make(map[string]SkillInfo, len(all))
	for _, s := range all {
		index[s.Name] = s
	}
	out := make([]SkillInfo, 0, len(names))
	for _, n := range names {
		s, ok := index[n]
		if !ok {
			available := make([]string, 0, len(all))
			for _, s := range all {
				available = append(available, s.Name)
			}
			return nil, fmt.Errorf("unknown skill %q (available: %s)", n, strings.Join(available, ", "))
		}
		out = append(out, s)
	}
	return out, nil
}

// installOne installs a single skill onto a single platform, dispatching on
// whether the platform is folder-based or single-file.
func installOne(platform Platform, skill SkillInfo, destRoot string, opts InstallOptions) (InstallResult, error) {
	dest, err := platform.Destination(destRoot, skill.Name)
	if err != nil {
		return InstallResult{}, err
	}
	result := InstallResult{Platform: platform, Skill: skill.Name, Destination: dest}

	if platform.IsSingleFile() {
		fileRes, err := installSingleFile(skill, dest, opts)
		if err != nil {
			return result, err
		}
		result.Files = []FileResult{fileRes}
		return result, nil
	}

	for _, rel := range skill.Files {
		fileRes, err := installFile(skill, rel, filepath.Join(dest, rel), opts)
		if err != nil {
			return result, err
		}
		result.Files = append(result.Files, fileRes)
	}
	return result, nil
}

// installFile renders one source file from the skill and writes it to the
// destination path, honoring Force and DryRun.
func installFile(skill SkillInfo, rel, destPath string, opts InstallOptions) (FileResult, error) {
	src := path(skill.src.name, rel)
	data, err := fs.ReadFile(skill.src.fsys, src)
	if err != nil {
		return FileResult{}, fmt.Errorf("reading skill file %q: %w", src, err)
	}
	rendered, err := renderIfMarkdown(rel, data, opts.TemplateContext)
	if err != nil {
		return FileResult{}, err
	}
	return writeFile(destPath, rendered, opts)
}

// installSingleFile concatenates all markdown files of a skill into one
// merged file (used by single-file platforms such as copilot).
func installSingleFile(skill SkillInfo, destPath string, opts InstallOptions) (FileResult, error) {
	merged, err := mergeSkill(skill, opts.TemplateContext)
	if err != nil {
		return FileResult{}, err
	}
	return writeFile(destPath, merged, opts)
}

// mergeSkill concatenates a skill's files into a single document with a YAML
// frontmatter derived from SKILL.md and section dividers between files.
func mergeSkill(skill SkillInfo, ctx TemplateContext) ([]byte, error) {
	var (
		frontmatter map[string]any
		bodyParts   []string
	)
	for _, rel := range skill.Files {
		src := path(skill.src.name, rel)
		raw, err := fs.ReadFile(skill.src.fsys, src)
		if err != nil {
			return nil, fmt.Errorf("reading skill file %q: %w", src, err)
		}
		rendered, err := renderIfMarkdown(rel, raw, ctx)
		if err != nil {
			return nil, err
		}
		fm, body := splitFrontmatter(rendered)
		if rel == "SKILL.md" && fm != nil {
			frontmatter = fm
		}
		if !strings.HasSuffix(strings.ToLower(rel), ".md") {
			// Non-markdown files are not merged; reference them by name only.
			bodyParts = append(bodyParts, fmt.Sprintf("<!-- skipped non-markdown file: %s -->", rel))
			continue
		}
		header := ""
		if rel != "SKILL.md" {
			header = fmt.Sprintf("\n\n<!-- source: %s -->\n\n", rel)
		}
		bodyParts = append(bodyParts, header+strings.TrimRight(string(body), "\n")+"\n")
	}

	var out strings.Builder
	if frontmatter == nil {
		frontmatter = map[string]any{}
	}
	if _, ok := frontmatter["applyTo"]; !ok {
		frontmatter["applyTo"] = "**"
	}
	fmBytes, err := yaml.Marshal(frontmatter)
	if err != nil {
		return nil, fmt.Errorf("marshaling merged frontmatter: %w", err)
	}
	out.WriteString("---\n")
	out.Write(fmBytes)
	out.WriteString("---\n")
	out.WriteString(strings.Join(bodyParts, "\n"))
	return []byte(out.String()), nil
}

// renderIfMarkdown applies template rendering to .md / .tmpl files; other
// files are returned verbatim.
func renderIfMarkdown(rel string, data []byte, ctx TemplateContext) ([]byte, error) {
	lower := strings.ToLower(rel)
	if strings.HasSuffix(lower, ".md") || strings.HasSuffix(lower, ".tmpl") || strings.HasSuffix(lower, ".tmpl.md") {
		return Render(rel, data, ctx)
	}
	return data, nil
}

// writeFile writes data to destPath, respecting DryRun and Force.
func writeFile(destPath string, data []byte, opts InstallOptions) (FileResult, error) {
	if !opts.Force {
		if _, err := os.Stat(destPath); err == nil {
			return FileResult{Path: destPath, Skipped: true, Reason: "destination exists (use --force to overwrite)"}, nil
		}
	}
	if opts.DryRun {
		return FileResult{Path: destPath, Skipped: true, Reason: "dry-run"}, nil
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return FileResult{}, fmt.Errorf("mkdir %q: %w", filepath.Dir(destPath), err)
	}
	if err := os.WriteFile(destPath, data, 0o600); err != nil {
		return FileResult{}, fmt.Errorf("writing %q: %w", destPath, err)
	}
	return FileResult{Path: destPath}, nil
}

// parseFrontmatter returns the decoded YAML frontmatter of a markdown file,
// or nil when no frontmatter block is present.
func parseFrontmatter(data []byte) map[string]any {
	fm, _ := splitFrontmatter(data)
	return fm
}

// splitFrontmatter separates the YAML frontmatter (delimited by leading "---")
// from the body. When no frontmatter exists, the first return is nil and the
// second is the original input.
func splitFrontmatter(data []byte) (out map[string]any, body []byte) {
	s := string(data)
	if !strings.HasPrefix(s, "---\n") && !strings.HasPrefix(s, "---\r\n") {
		return nil, data
	}
	// Find the closing delimiter.
	rest := s[4:]
	end := strings.Index(rest, "\n---")
	if end < 0 {
		return nil, data
	}
	fmText := rest[:end]
	bodyStart := end + len("\n---")
	if bodyStart < len(rest) && (rest[bodyStart] == '\n' || rest[bodyStart] == '\r') {
		bodyStart++
		if bodyStart < len(rest) && rest[bodyStart-1] == '\r' && rest[bodyStart] == '\n' {
			bodyStart++
		}
	}
	body = []byte(rest[bodyStart:])
	if err := yaml.Unmarshal([]byte(fmText), &out); err != nil {
		return nil, data
	}
	return out, body
}

// path joins embedded-FS path components using forward slashes (embed always
// uses slash-delimited paths, regardless of host OS).
func path(parts ...string) string {
	return strings.Join(parts, "/")
}
