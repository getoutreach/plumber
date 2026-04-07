{{define "plumber/command/shape/struct/comment"}}{{end}}
{{define "plumber/command/shape/field/comment"}}{{end}}
{{define "plumber/command/shape/file/copyright"}}{{template "plumber/command/file/copyright" $}}{{end}}
{{define "plumber/command/shape/file/comment"}}{{ if eq $.Scope.Mode "generated"}}// Generated file by plumber shape function. DON'T edit manually.
{{end}}{{end}}
{{define "plumber/command/shape/file/content"}}
    {{ template "plumber/command/shape/file/copyright" $ -}}
    {{ template "plumber/command/shape/file/comment" $ -}}
    package {{ $.Scope.Package.Name }}
    {{ template "plumber/command/file/imports" $ -}}
    {{ placeholder "header" }}
    {{ .Scope.Content }}
    {{ placeholder "footer" }}
{{end}}
{{define "plumber/command/shape/field/type" -}}
{{ type_wrap $.Scope.Subject $.Scope.Field.Type.Spec }}
{{end}}
{{define "plumber/command/shape/field" -}}
{{ template "plumber/command/shape/field/comment" $ -}}
    {{ $.Scope.Field.Name }} {{ template "plumber/command/shape/field/type" $ -}}
{{end}}
{{define "plumber/command/shape"}}
{{ template "plumber/command/shape/struct/comment" . -}}
{{ $name := or (annotation_value $.Scope.Subject "plumber:name") .Type.Name }}
// {{ $name }} is shaped from {{ $.Type.Spec.FQN }}.
{{ comment $.Scope.Subject -}}
type {{ $name }} struct {
}
{{end}}
