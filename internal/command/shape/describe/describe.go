// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file implements the core describe logic that extracts macro, option, and handler
// metadata from a fully-merged shape configuration into a structured Description for output formatting.

// Package describe provides introspection of a shape configuration, extracting registered macros,
// options, and handlers into a structured description that can be rendered in multiple output formats.
package describe

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/getoutreach/plumber/internal/command/shape"
	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/getoutreach/plumber/internal/command/shape/expand"
	"github.com/getoutreach/plumber/internal/command/shape/structure"
	"github.com/getoutreach/plumber/internal/command/template"
	"github.com/samber/lo"
)

// Description is the top-level output structure containing all registered
// shape configuration objects grouped by category.
type Description struct {
	Macros   []MacroDescription   `json:"macros" yaml:"macros"`
	Options  []OptionDescription  `json:"options" yaml:"options"`
	Handlers []HandlerDescription `json:"handlers" yaml:"handlers"`
}

// MacroDescription describes a single registered macro.
type MacroDescription struct {
	Name      string                `json:"name" yaml:"name"`
	Doc       DocDescription        `json:"doc" yaml:"doc"`
	Metadata  MetadataDescription   `json:"metadata" yaml:"metadata"`
	Schema    *SchemaDescription    `json:"schema,omitempty" yaml:"schema,omitempty"`
	Options   []string              `json:"options,omitempty" yaml:"options,omitempty"`
	Structure *StructureDescription `json:"structure,omitempty" yaml:"structure,omitempty"`
}

// OptionDescription describes a single registered annotation option.
type OptionDescription struct {
	Name      string                `json:"name" yaml:"name"`
	Doc       DocDescription        `json:"doc" yaml:"doc"`
	Metadata  MetadataDescription   `json:"metadata" yaml:"metadata"`
	Schema    *SchemaDescription    `json:"schema,omitempty" yaml:"schema,omitempty"`
	Structure *StructureDescription `json:"structure,omitempty" yaml:"structure,omitempty"`
}

// HandlerDescription describes a single registered handler.
type HandlerDescription struct {
	Name    string         `json:"name" yaml:"name"`
	Command string         `json:"command" yaml:"command"`
	Args    []string       `json:"args,omitempty" yaml:"args,omitempty"`
	Doc     DocDescription `json:"doc" yaml:"doc"`
}

// HandlerVariantDescription describes a named variant of a handler.
type HandlerVariantDescription struct {
	Name    string   `json:"name" yaml:"name"`
	Command string   `json:"command" yaml:"command"`
	Args    []string `json:"args,omitempty" yaml:"args,omitempty"`
}

// DocDescription holds the description and usage documentation fields.
type DocDescription struct {
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	Usage       string `json:"usage,omitempty" yaml:"usage,omitempty"`
}

// StructureDescription holds the resolved structure name and path.
type StructureDescription struct {
	Name string `json:"name" yaml:"name"`
	Path string `json:"path" yaml:"path"`
}

// MetadataDescription holds metadata fields for an option or macro.
type MetadataDescription struct {
	Source   *SourceDescription `json:"source,omitempty" yaml:"source,omitempty"`
	Singular bool               `json:"singular" yaml:"singular"`
	Handler  string             `json:"handler,omitempty" yaml:"handler,omitempty"`
}

// SourceDescription holds structured source provenance information.
type SourceDescription struct {
	Repository string `json:"repository" yaml:"repository"`
	Ref        string `json:"ref" yaml:"ref"`
}

// SchemaDescription holds introspected positional and named argument schemas.
type SchemaDescription struct {
	Positional *PositionalSchema `json:"positional,omitempty" yaml:"positional,omitempty"`
	Named      *NamedSchema      `json:"named,omitempty" yaml:"named,omitempty"`
}

// PositionalSchema describes positional (array) arguments with introspected items.
type PositionalSchema struct {
	Items []ArgumentItem `json:"items" yaml:"items"`
	Raw   interface{}    `json:"raw" yaml:"raw"`
}

// ArgumentItem describes a single positional argument slot.
type ArgumentItem struct {
	Position    string `json:"position" yaml:"position"`
	Type        string `json:"type" yaml:"type"`
	Description string `json:"description" yaml:"description"`
	Required    bool   `json:"required" yaml:"required"`
	Details     string `json:"details,omitempty" yaml:"details,omitempty"`
}

// NamedSchema describes named (object) arguments with introspected properties.
type NamedSchema struct {
	Properties []NamedArgItem `json:"properties" yaml:"properties"`
	Raw        interface{}    `json:"raw" yaml:"raw"`
}

// NamedArgItem describes a single named argument property.
type NamedArgItem struct {
	Name        string `json:"name" yaml:"name"`
	Type        string `json:"type" yaml:"type"`
	Description string `json:"description" yaml:"description"`
	Required    bool   `json:"required" yaml:"required"`
	Details     string `json:"details,omitempty" yaml:"details,omitempty"`
}

// FunctionDescriptions is node describing a function
type FunctionDescription struct {
	Name    string              `json:"name" yaml:"name"`
	Doc     DocDescription      `json:"doc" yaml:"doc"`
	Params  []ParamDescription  `json:"params,omitempty" yaml:"params,omitempty"`
	Results []ResultDescription `json:"results,omitempty" yaml:"results,omitempty"`
}

// ParamDescription describes a single input parameter of a template function.
// Type is the fully qualified name (FQN) representation of the parameter's
// Go type (predeclared types are bare; named types use the
// quoted-package-path form, e.g. *"net/url".URL).
type ParamDescription struct {
	Type     string `json:"type" yaml:"type"`
	Variadic bool   `json:"variadic,omitempty" yaml:"variadic,omitempty"`
}

// ResultDescription describes a single result of a template function.
// Type is the fully qualified name (FQN) of the result's Go type.
type ResultDescription struct {
	Type string `json:"type" yaml:"type"`
}

// Build extracts macros, options, and handlers from the given shape config
// and returns a Description ready for formatting. The resolver is used to
// resolve structure paths referenced by annotations.
func Build(cfg *shape.Config, resolver contract.StructurePathResolver) Description {
	return Description{
		Macros:   buildMacros(cfg.Macros, cfg.Options, resolver),
		Options:  buildOptions(cfg.Options, resolver),
		Handlers: buildHandlers(cfg.Handlers),
	}
}

func buildMacros(
	macros []config.MacroConfig,
	options []config.AnnotationSchemaConfig,
	resolver contract.StructurePathResolver,
) []MacroDescription {
	// Build a lookup from option name to handler for resolving macro option handlers.
	optionHandlers := make(map[string]string, len(options))
	for _, o := range options {
		if o.Handler != "" {
			optionHandlers[o.Name] = o.Handler
		}
	}

	result := make([]MacroDescription, 0, len(macros))
	for _, m := range macros {
		if m.PlumberMacro == nil {
			continue
		}
		pm := m.PlumberMacro

		expand.MacroConfig(pm)

		// Collect handlers from referenced options.
		var handlers = []string{pm.Handler}
		for _, opt := range pm.Annotations {
			if h, ok := optionHandlers[opt.Name]; ok {
				handlers = append(handlers, h)
			}
		}

		h, _ := lo.First(lo.Compact(lo.Uniq(handlers)))

		md := MacroDescription{
			Name: pm.Name,
			Doc:  DocDescription{Description: pm.Doc.Description, Usage: pm.Doc.Usage},
			Metadata: MetadataDescription{
				Source:   buildSource(pm.Git),
				Singular: pm.Singular,
				Handler:  h,
			},
			Schema:    buildSchema(pm.Schema),
			Options:   buildOptionRefs(pm.Options),
			Structure: buildStructure(pm.Structure, resolver),
		}
		result = append(result, md)
	}
	return result
}

func buildOptions(options []config.AnnotationSchemaConfig, resolver contract.StructurePathResolver) []OptionDescription {
	result := make([]OptionDescription, 0, len(options))
	for _, o := range options {
		od := OptionDescription{
			Name: o.Name,
			Doc:  DocDescription{Description: o.Doc.Description, Usage: o.Doc.Usage},
			Metadata: MetadataDescription{
				Source:   buildSource(o.Git),
				Singular: o.Singular,
				Handler:  o.Handler,
			},
			Schema:    buildSchema(o.Schema),
			Structure: buildStructure(o.Structure, resolver),
		}
		result = append(result, od)
	}
	return result
}

// buildSource converts a GitSourceConfig into a SourceDescription, or nil if not from git.
func buildSource(git *template.GitSourceConfig) *SourceDescription {
	if git == nil {
		return nil
	}
	return &SourceDescription{
		Repository: git.Repository,
		Ref:        git.Ref,
	}
}

// buildStructure resolves a structure reference into a StructureDescription with
// the name (stripped of the structure: prefix) and the resolved filesystem path.
// Returns nil if the raw structure string is empty.
func buildStructure(raw string, resolver contract.StructurePathResolver) *StructureDescription {
	if raw == "" {
		return nil
	}
	name := strings.TrimPrefix(raw, structure.StructurePathPrefix)
	resolvedPath, err := resolver.ResolveStructurePath(raw)
	if err != nil {
		resolvedPath = raw
	}
	return &StructureDescription{
		Name: name,
		Path: resolvedPath,
	}
}

func buildHandlers(handlers []config.HandlerConfig) []HandlerDescription {
	result := make([]HandlerDescription, 0, len(handlers))
	for _, h := range handlers {
		if h.PlumberHandler == nil {
			continue
		}
		hd := HandlerDescription{
			Name:    h.PlumberHandler.Name,
			Command: h.PlumberHandler.Command,
			Args:    h.PlumberHandler.Args,
			Doc:     DocDescription{Description: h.PlumberHandler.Doc.Description, Usage: h.PlumberHandler.Doc.Usage},
		}
		result = append(result, hd)
	}
	return result
}

func buildSchema(schema *config.AnnotationArgumentSchemaConfig) *SchemaDescription {
	if schema == nil {
		return nil
	}
	positional := introspectPositional(&schema.Positional)
	named := introspectNamed(&schema.Named)
	if positional == nil && named == nil {
		return nil
	}
	return &SchemaDescription{
		Positional: positional,
		Named:      named,
	}
}

func buildOptionRefs(opts []config.OptionReferenceConfig) []string {
	if len(opts) == 0 {
		return nil
	}
	result := make([]string, 0, len(opts))
	for _, o := range opts {
		result = append(result, o.Name)
	}
	return result
}

// introspectPositional parses a positional (array) schema node into structured items.
func introspectPositional(node *yaml.Node) *PositionalSchema {
	raw := decodeNode(node)
	if raw == nil {
		return nil
	}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return &PositionalSchema{Raw: raw}
	}

	ps := &PositionalSchema{Raw: raw}

	// Determine minItems for required calculation
	minItems := 0
	if v, ok := m["minItems"]; ok {
		minItems = toInt(v)
	}

	// Extract items — can be a single object (homogeneous) or array (tuple/prefixItems)
	items := extractItems(m, minItems)
	ps.Items = items

	return ps
}

// introspectNamed parses a named (object) schema node into structured properties.
func introspectNamed(node *yaml.Node) *NamedSchema {
	raw := decodeNode(node)
	if raw == nil {
		return nil
	}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return &NamedSchema{Raw: raw}
	}

	ns := &NamedSchema{Raw: raw}

	// Determine required fields
	requiredSet := make(map[string]bool)
	if req, ok := m["required"].([]interface{}); ok {
		for _, r := range req {
			if s, ok := r.(string); ok {
				requiredSet[s] = true
			}
		}
	}

	// Extract properties
	if props, ok := m["properties"].(map[string]interface{}); ok {
		for name, v := range props {
			propMap, _ := v.(map[string]interface{})
			item := NamedArgItem{
				Name:        name,
				Type:        strField(propMap, "type"),
				Description: strField(propMap, "description"),
				Required:    requiredSet[name],
				Details:     buildDetails(propMap),
			}
			ns.Properties = append(ns.Properties, item)
		}
	}

	// Handle additionalProperties as a catch-all
	if addProps, ok := m["additionalProperties"].(map[string]interface{}); ok {
		item := NamedArgItem{
			Name:        "*",
			Type:        strField(addProps, "type"),
			Description: strField(addProps, "description"),
			Details:     buildDetails(addProps),
		}
		ns.Properties = append(ns.Properties, item)
	}

	return ns
}

// extractItems handles both homogeneous (single object) and tuple (array) items schemas.
func extractItems(m map[string]interface{}, minItems int) []ArgumentItem {
	itemsRaw, ok := m["items"]
	if !ok {
		// Try prefixItems (JSON Schema draft 2020-12)
		itemsRaw, ok = m["prefixItems"]
		if !ok {
			return nil
		}
	}

	switch v := itemsRaw.(type) {
	case map[string]interface{}:
		// Homogeneous — all positions share the same schema
		return []ArgumentItem{{
			Position:    "*",
			Type:        strField(v, "type"),
			Description: strField(v, "description"),
			Required:    minItems > 0,
			Details:     buildDetails(v),
		}}
	case []interface{}:
		// Tuple — each element is a separate positional arg
		items := make([]ArgumentItem, 0, len(v))
		for i, elem := range v {
			em, _ := elem.(map[string]interface{})
			items = append(items, ArgumentItem{
				Position:    fmt.Sprintf("%d", i),
				Type:        strField(em, "type"),
				Description: strField(em, "description"),
				Required:    i < minItems,
				Details:     buildDetails(em),
			})
		}
		return items
	default:
		return nil
	}
}

// buildDetails constructs a human-readable details string from schema constraints.
func buildDetails(m map[string]interface{}) string {
	if m == nil {
		return ""
	}
	var parts []string

	// Enum values
	if enum, ok := m["enum"].([]interface{}); ok && len(enum) > 0 {
		vals := make([]string, 0, len(enum))
		for _, v := range enum {
			vals = append(vals, fmt.Sprintf("%v", v))
		}
		parts = append(parts, "enum: "+strings.Join(vals, ", "))
	}

	// Default value
	if def, ok := m["default"]; ok {
		parts = append(parts, fmt.Sprintf("default: %v", def))
	}

	// OneOf constraint
	if oneOf, ok := m["oneOf"].([]interface{}); ok && len(oneOf) > 0 {
		descs := make([]string, 0, len(oneOf))
		for _, o := range oneOf {
			om, _ := o.(map[string]interface{})
			if req, ok := om["required"].([]interface{}); ok {
				reqStrs := make([]string, 0, len(req))
				for _, r := range req {
					reqStrs = append(reqStrs, fmt.Sprintf("%v", r))
				}
				descs = append(descs, "requires: "+strings.Join(reqStrs, ", "))
			}
		}
		if len(descs) > 0 {
			parts = append(parts, "oneOf["+strings.Join(descs, " | ")+"]")
		}
	}

	return strings.Join(parts, "; ")
}

// strField extracts a string field from a map, returning "" if missing or wrong type.
func strField(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

// toInt converts an interface{} (typically decoded from YAML as int or float64) to int.
func toInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	default:
		return 0
	}
}

// decodeNode converts a yaml.Node to an interface{} value for serialization.
// Returns nil for zero-value or null nodes.
func decodeNode(node *yaml.Node) interface{} {
	if node == nil || node.Kind == 0 || node.Tag == "!!null" {
		return nil
	}
	var out interface{}
	if err := node.Decode(&out); err != nil {
		return nil
	}
	return out
}
