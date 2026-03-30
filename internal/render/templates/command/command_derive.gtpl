{{define "plumber/command/derive/file/copyright"}}{{end}}
{{define "plumber/command/derive/file/comment"}}{{ if eq $.Scope.Mode "generated"}}// Generated file by plumber shape function. DON'T edit manually.
{{end}}{{end}}
{{define "plumber/command/derive/file/imports" -}}
    {{ if $.Scope.Modules.Imports -}}
        import (
        {{ range $i := $.Scope.Modules.Imports -}}
        {{ $i }}
        {{ end -}}
        // <<plumber::Block(imports)>>
        // <</plumber::Block>>
        )
    {{end}}
{{end}}
{{define "plumber/command/derive/file/content"}}
    {{ template "plumber/command/derive/file/copyright" $ -}}
    {{ template "plumber/command/derive/file/comment" $ -}}
    package {{ $.Scope.Package.Name }}
    {{ template "plumber/command/derive/file/imports" $ -}}
    // <<plumber::Block(header)>>
    // <</plumber::Block>>
    {{ .Scope.Content }}
    // <<plumber::Block(footer)>>
    // <</plumber::Block>>
{{end}}
{{define "plumber/command/derive/struct/comment"}}{{end}}
{{define "plumber/command/derive/field/comment"}}{{end}}
{{define "plumber/command/derive/field/type" -}}
{{ type $.Scope.Field.Type.Spec }}
{{end}}
{{define "plumber/command/derive/field" -}}
{{ template "plumber/command/derive/field/comment" $ -}}
    {{ $.Scope.Field.Name }} {{ template "plumber/command/derive/field/type" $ -}}
{{end}}
{{define "plumber/command/derive"}}
{{ template "plumber/command/derive/struct/comment" . -}}
{{ $name := or (annotation_value $.Scope.Derive "plumber:name") .Type.Name }}
// {{ $name }} is derived from {{ $.Type.Spec.FQN }}.
// You can customize it but some fields may be automatically re-introduced based on the original struct definition.
{{ comment $.Scope.Derive -}}
type {{ $name }} struct {
    {{ range $f := filter_elements $.Scope.Derive $.Type.Struct.Fields "fields" -}}
        {{ if (ignored $f.Name) -}}{{ continue -}}{{ end -}}
        {{ with $scope := extend $ "Field" $f -}}{{template "plumber/command/derive/field" $scope -}}{{- end -}}
    {{- end -}}
}
{{end}}
