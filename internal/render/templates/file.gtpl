{{define "plumber/file/comment"}}{{ if eq $.Scope.Mode "generated"}}// Generated file by plumber shape function. DON'T edit manually.
{{end}}{{end}}
{{define "plumber/file/package_description" -}}
{{ if $.Scope.File.PackageDescription -}}
// Package {{ $.Scope.Package.Name }} {{ $.Scope.File.PackageDescription }}
{{- end}}
{{end}}

{{define "plumber/file/description" -}}
{{ if $.Scope.File.Description -}}
// Description: {{ $.Scope.File.Description }}
{{end}}
{{end}}

{{define "plumber/file/documentation" -}}
{{ if $.Scope.File.Documentation -}}
{{ comment_wrap $.Scope.File.Documentation }}
{{end}}
{{end}}


{{define "plumber/file/content"}}
    {{ template "plumber/file/copyright" $ -}}
    {{ template "plumber/file/description" $ -}}
    {{ template "plumber/file/comment" $ -}}
    {{ template "plumber/file/package_description" $ -}}
    package {{ $.Scope.Package.Name }}
    {{ template "plumber/file/imports" $ -}}
    {{ template "plumber/file/documentation" $ -}}

    {{ placeholder "header" }}
    {{ .Scope.Content }}
    {{ placeholder "footer" }}
{{end}}
{{define "plumber/file/copyright"}}{{end}}
{{define "plumber/file/imports" -}}
    {{ if $.Scope.Modules.Imports -}}
        import (
        {{ range $i := $.Scope.Modules.Imports -}}
        {{ $i }}
        {{ end -}}
        {{ placeholder "imports" -}}
        )
    {{end}}
{{end}}
{{define "plumber/empty" -}}
{{end}}
