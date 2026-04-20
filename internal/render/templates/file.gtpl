{{define "plumber/file/comment"}}{{ if eq $.Scope.Mode "generated"}}// Generated file by plumber shape function. DON'T edit manually.
{{end}}{{end}}
{{define "plumber/file/content"}}
    {{ template "plumber/file/copyright" $ -}}
    {{ template "plumber/file/comment" $ -}}
    package {{ $.Scope.Package.Name }}
    {{ template "plumber/file/imports" $ -}}
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
        // <<plumber::Block(imports)>>
        // <</plumber::Block>>
        )
    {{end}}
{{end}}
