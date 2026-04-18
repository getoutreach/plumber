{{define "plumber/command/file/copyright"}}{{end}}
{{define "plumber/command/file/imports" -}}
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
