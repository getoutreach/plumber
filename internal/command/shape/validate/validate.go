// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file provides JSON Schema-based validation for annotation arguments
// using compiled schemas from the shape configuration.

// Package validate provides JSON Schema compilation and validation for plumber annotation
// positional and named arguments, ensuring annotations conform to their declared schemas.
package validate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/getoutreach/plumber/internal/command/shape/config"
	"github.com/getoutreach/plumber/query/model"
	"github.com/santhosh-tekuri/jsonschema"
	"gopkg.in/yaml.v3"
)

// CompiledSchema holds pre-compiled JSON Schema objects for validating
// an annotation's positional and named arguments independently.
type CompiledSchema struct {
	Name       string
	Positional *jsonschema.Schema // nil if no positional schema defined
	Named      *jsonschema.Schema // nil if no named schema defined
}

// CompileSchema converts an AnnotationArgumentSchemaConfig into a CompiledSchema
// by marshalling the yaml.Node fields to JSON and compiling them with the
// jsonschema compiler.
func CompileSchema(name string, schema *config.AnnotationArgumentSchemaConfig) (*CompiledSchema, error) {
	if schema == nil {
		return nil, nil
	}

	cs := &CompiledSchema{Name: name}

	if schema.Positional.Kind != 0 {
		s, err := compileYAMLNode(name+"/positional", &schema.Positional)
		if err != nil {
			return nil, fmt.Errorf("compiling positional schema for %q: %w", name, err)
		}
		cs.Positional = s
	}

	if schema.Named.Kind != 0 {
		s, err := compileYAMLNode(name+"/named", &schema.Named)
		if err != nil {
			return nil, fmt.Errorf("compiling named schema for %q: %w", name, err)
		}
		cs.Named = s
	}

	return cs, nil
}

// CompileSchemas compiles a list of AnnotationSchemaConfig entries into a map
// of CompiledSchema keyed by annotation name. Entries without a schema are skipped.
func CompileSchemas(schemas []config.AnnotationSchemaConfig) (map[string]*CompiledSchema, error) {
	result := make(map[string]*CompiledSchema, len(schemas))
	for _, s := range schemas {
		if s.Schema == nil {
			continue
		}
		cs, err := CompileSchema(s.Name, s.Schema)
		if err != nil {
			return nil, err
		}
		if cs != nil {
			result[s.Name] = cs
		}
	}
	return result, nil
}

// Annotation validates an annotation's Args and NamedArgs against the
// given compiled schema. Returns nil if valid or if the schema is nil.
func Annotation(ann model.Annotation, schema *CompiledSchema) error {
	if schema == nil {
		return nil
	}

	var errs []string

	if schema.Positional != nil {
		doc := toInterfaceSlice(ann.Args)
		if err := schema.Positional.ValidateInterface(doc); err != nil {
			errs = append(errs, fmt.Sprintf("positional args: %s", formatValidationError(err)))
		}
	}

	if schema.Named != nil {
		doc := toInterfaceMap(ann.NamedArgs)
		if err := schema.Named.ValidateInterface(doc); err != nil {
			errs = append(errs, fmt.Sprintf("named args: %s", formatValidationError(err)))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("annotation %q validation failed: %s", ann.Name, strings.Join(errs, "; "))
	}
	return nil
}

// Annotations validates each annotation in the list against the matching
// compiled schema (looked up by annotation name). Annotations without a matching
// schema are skipped. Returns the first validation error encountered.
func Annotations(annotations model.Annotations, schemas map[string]*CompiledSchema) error {
	if len(schemas) == 0 {
		return nil
	}
	for _, ann := range annotations {
		schema, ok := schemas[ann.Name]
		if !ok {
			continue
		}
		if err := Annotation(ann, schema); err != nil {
			return err
		}
	}
	return nil
}

// compileYAMLNode converts a yaml.Node to a compiled JSON Schema by:
// 1. Marshalling the yaml.Node to YAML bytes
// 2. Unmarshalling to interface{} (producing map[string]interface{})
// 3. Marshalling to JSON bytes
// 4. Feeding to the jsonschema compiler
func compileYAMLNode(id string, node *yaml.Node) (*jsonschema.Schema, error) {
	// Step 1: yaml.Node → YAML bytes
	yamlBytes, err := yaml.Marshal(node)
	if err != nil {
		return nil, fmt.Errorf("marshalling yaml.Node: %w", err)
	}

	// Step 2: YAML bytes → interface{}
	var doc interface{}
	if err := yaml.Unmarshal(yamlBytes, &doc); err != nil {
		return nil, fmt.Errorf("unmarshalling YAML to interface: %w", err)
	}

	// Step 3: Normalize for JSON Schema (convert map[string]interface{} values,
	// ensure integers are json.Number compatible, etc.)
	doc = normalizeForJSON(doc)

	// Step 4: interface{} → JSON bytes
	jsonBytes, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshalling to JSON: %w", err)
	}

	// Step 5: Compile with jsonschema
	url := "mem://" + id
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(url, bytes.NewReader(jsonBytes)); err != nil {
		return nil, fmt.Errorf("adding schema resource: %w", err)
	}

	schema, err := compiler.Compile(url)
	if err != nil {
		return nil, fmt.Errorf("compiling schema: %w", err)
	}

	return schema, nil
}

// toInterfaceSlice converts []string to []interface{} for JSON Schema validation.
func toInterfaceSlice(args []string) interface{} {
	if args == nil {
		return []interface{}{}
	}
	result := make([]interface{}, len(args))
	for i, v := range args {
		result[i] = v
	}
	return result
}

// toInterfaceMap converts map[string]string to map[string]interface{} for JSON Schema validation.
func toInterfaceMap(namedArgs map[string]string) interface{} {
	if namedArgs == nil {
		return map[string]interface{}{}
	}
	result := make(map[string]interface{}, len(namedArgs))
	for k, v := range namedArgs {
		result[k] = v
	}
	return result
}

// normalizeForJSON recursively converts YAML-decoded values to JSON-compatible types.
// In particular, it converts map[interface{}]interface{} to map[string]interface{}
// and int values to json.Number so the jsonschema library can process them correctly.
func normalizeForJSON(v interface{}) interface{} {
	switch v := v.(type) {
	case map[string]interface{}:
		for k, val := range v {
			v[k] = normalizeForJSON(val)
		}
		return v
	case map[interface{}]interface{}:
		m := make(map[string]interface{}, len(v))
		for k, val := range v {
			m[fmt.Sprintf("%v", k)] = normalizeForJSON(val)
		}
		return m
	case []interface{}:
		for i, val := range v {
			v[i] = normalizeForJSON(val)
		}
		return v
	case int:
		return json.Number(fmt.Sprintf("%d", v))
	case int64:
		return json.Number(fmt.Sprintf("%d", v))
	case float64:
		return json.Number(fmt.Sprintf("%g", v))
	default:
		return v
	}
}

// formatValidationError extracts a human-readable message from a jsonschema
// validation error, unwrapping nested causes when available.
func formatValidationError(err error) string {
	var ve *jsonschema.ValidationError
	if errors.As(err, &ve) {
		if len(ve.Causes) > 0 {
			msgs := make([]string, 0, len(ve.Causes))
			for _, cause := range ve.Causes {
				msgs = append(msgs, formatValidationError(cause))
			}
			return strings.Join(msgs, "; ")
		}
		if ve.Message != "" {
			return ve.Message
		}
	}
	return err.Error()
}
