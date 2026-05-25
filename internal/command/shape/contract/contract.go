// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines annotation option constants used by the shape command.

// Package contract defines the annotation constants for the plumber shape command.
package contract

import (
	"context"

	"github.com/getoutreach/plumber/internal/genius/gen"
	"github.com/getoutreach/plumber/query/model"
)

// Option constants for annotations used in plumber templates and code generation.
const (
	// OptionTemplate specifies the template to use for code generation.
	OptionTemplate = "plumber:template"
	// OptionIgnore specifies a template to ignore, allowing for selective exclusion of templates during code generation.
	OptionIgnore = "plumber:ignore"
	// OptionContext specifies the context for the transformation. It supports two modes:
	//   - Single type: plumber:context "pkg/path".TypeName — targets a single type by FQN.
	//   - Package matcher: plumber:context pkg/path matcher=<name> — targets all types in the
	//     package that match the named matcher's rules (kind, fqn, or annotation-based).
	OptionContext = "plumber:context"
	// OptionComment allows adding a comment to the generated code, which can be used for documentation or clarification purposes.
	OptionComment = "plumber:comment"
	// OptionName specifies a custom name for the generated type or function, overriding the default naming convention.
	OptionName = "plumber:name"
	// OptionReceiver specifies the receiver type for generated methods, allowing for method generation on specific types.
	OptionReceiver = "plumber:receiver"
	// OptionFilter specifies a filter to apply to the node's fields or methods,
	// enabling selective code generation based on field or method names, types, or other criteria.
	OptionFilter = "plumber:filter"
	// OptionMixin specifies a mixin template to include in the generated code,
	// allowing for code reuse and composition across different templates.
	OptionMixin = "plumber:mixin"
	// OptionOutput specifies the output path for the generated code, allowing for flexible file organization and output management.
	OptionOutput = "plumber:output"
	// OptionMode specifies the mode of code generation, such as "generate" for
	// generating new files or "inplace" for modifying existing files in place.
	OptionMode = "plumber:mode"
	// OptionFieldWrapper specifies a template to wrap struct fields, enabling custom
	// field-level code generation such as adding tags, validation, or other field-specific logic.
	OptionFieldWrapper = "plumber:field_wrapper"
	// OptionQuery specifies a query entry point that searches for entities matching a regex pattern
	// within a defined scope and populates an annotated slice variable with compatible results.
	OptionQuery = "plumber:query"
	// OptionScope injects a resolved type into the template scope under .Scope.Custom.<name>,
	// allowing templates to access additional type information beyond the subject type.
	// Usage: plumber:scope "MyType" type="pkg/path".TypeName
	OptionScope = "plumber:scope"
	// OptionDependsOn declares a hard dependency on another type by FQN. The
	// transformation is silently skipped (no error) when the referenced type cannot be
	// resolved within the inspected packages. The annotation may be specified multiple
	// times; every dependency must resolve for the transformation to run.
	// Usage: plumber:depends_on "pkg/path".TypeName
	OptionDependsOn = "plumber:depends_on"
	// OptionNotify triggers a named handler with optional named arguments at the end
	// of the shape run. The first positional argument is the handler name; named
	// arguments are aggregated across all notifications targeting the same handler
	// and passed to the handler command template.
	// Usage: plumber:notify goverter path="generated/converters"
	OptionNotify = "plumber:notify"

	// TransformationDerive specifies a macro to derive a new type from the annotated node, allowing for custom type
	// transformations and generation based on the original node's structure and annotations.
	TransformationDerive = "plumber:derive"

	// TransformationShape specifies that the annotated node should be processed by the shape command, enabling
	// transformations based on the node's annotations and structure.
	TransformationShape = "plumber:shape"

	// TransformationRender specifies a template to render the annotated node, allowing for custom rendering of the node's
	// code based on a specified template.
	TransformationRender = "plumber:render"
)

// Packager defines an interface for retrieving the package information of a node,
// which can be used in transformations to access package-level details during code generation.
type Packager interface {
	GetPackage() *model.Package
}

// Transformer defines the interface for all transformers that can be applied to annotated nodes in the shape command.
type Transformer interface {
	Annotable
	Accepts(annotation string) bool
	Add(annotation model.Annotation)
	Validate(packager Packager) error
	GetPosition() model.Position
	GetPackage() *model.Package
	Output() string
	GetName() string
	Mode() string
}

// Annotable represents an entity that can have annotations, such as a struct or interface type in the AST.
type Annotable interface {
	GetAnnotations() model.Annotations
}

// Node represents a node in the AST that can be transformed by a Transformer, such as a struct or interface type.
type Node interface {
	Annotable
	GetPosition() model.Position
	GetPackage() *model.Package
}

// ReporterEventType defines the type of events that can be reported by the Reporter interface
type ReporterEventType string

// ReporterEventType constants for different types of events that can be reported by the Reporter interface.
const (
	// EventTransformerAdded is emitted when a transformer is added to a node.
	EventTransformerAdded ReporterEventType = "transformer.added"
	// EventTransformerSkipped is emitted when a transformer is skipped for a node.
	EventTransformerSkipped ReporterEventType = "transformer.skipped"
	// EventTransformerRestored is emitted when a transformer is restored for a node.
	EventTransformerRestored ReporterEventType = "transformer.restored"
	// EventTransformerError is emitted when an error occurs during transformer processing.
	EventTransformerError ReporterEventType = "transformer.error"
	// EventTransformerInfo is emitted for informational messages during transformer processing.
	EventTransformerInfo ReporterEventType = "transformer.info"
	// EventTransformerOutput is emitted when a transformer produces output, such as a generated file.
	EventTransformerOutput ReporterEventType = "transformer.output"
	// EventQueryExecuted is emitted when a query is executed, containing the query string and results.
	EventQueryExecuted ReporterEventType = "query.executed"
	// EventQueryError is emitted when an error occurs during query execution.
	EventQueryError ReporterEventType = "query.error"
	// EventHandlerTriggered is emitted when a plumber:notify annotation is processed, indicating
	// that a handler has been triggered by a transformer.
	EventHandlerTriggered ReporterEventType = "handler.triggered"
	// EventHandlerExecuting is emitted when a handler command is about to be executed.
	EventHandlerExecuting ReporterEventType = "handler.executing"
	// EventHandlerCompleted is emitted when a handler command finishes successfully.
	EventHandlerCompleted ReporterEventType = "handler.completed"
	// EventHandlerError is emitted when a handler command fails.
	EventHandlerError ReporterEventType = "handler.error"
)

// ReporterEvent represents an event that can be reported by the Reporter interface,
// containing information about the event type and any relevant data.
type ReporterEvent struct {
	Message     string
	Kind        ReporterEventType
	Transformer Transformer
	Node        model.Node
	Path        string
	Error       error
}

// Reporter defines the interface for reporting events during the transformation process,
// allowing for logging, debugging, or other side effects based on events such as adding a transformer to a node.
type Reporter interface {
	Notify(ReporterEvent)
}

// TemplateLoader resolves template names into render option functions.
// This is implemented by template.TemplateCache.
type TemplateLoader interface {
	Load(name string, names ...string) ([]gen.RenderOptionsFunc, error)
}

// StructurePathResolver defines an interface for resolving structure paths according to structure path configuration
type StructurePathResolver interface {
	ResolvePackagePath(path string) (string, error)
	ResolveStructurePath(path string) (string, error)
}

// ShapingContext provides context for a transformation, including a Reporter for emitting events during the transformation process.
type ShapingContext struct {
	context.Context
	Reporter              Reporter
	TemplateLoader        TemplateLoader
	StructurePathResolver StructurePathResolver
	Notifications         NotificationCollector
	BaseDir               string
	RepoModule            ModuleInfo
	Module                ModuleInfo
}

// ModuleInfo represents information about a Go module, including its name, normalized name, path, and relative path.
type ModuleInfo struct {
	Name           string
	NormalizedName string
	Path           string
	RelativePath   string
	Dir            string
}

// NotificationCollector collects handler notifications emitted by plumber:notify annotations
// during transformer execution. Implementations aggregate named arguments per handler name
// and execute the corresponding handler commands after all transformations complete.
type NotificationCollector interface {
	// Notify records a notification for the given handler name with the provided named arguments.
	Notify(handlerName string, namedArgs map[string]string)
	Execute(*ShapingContext) error
}
