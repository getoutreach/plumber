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
{{define "plumber/command/shape"}}
{{ $name := or (annotation_value $.Scope.Subject "plumber:name") .Type.Name }}
{{ with $scope := extend $ "Name" $name -}}
    {{ if $.Type.Interface -}}
        {{ template "plumber/command/shape/interface" $scope -}}
    {{ end }}
    {{ if $.Type.Struct -}}
        {{ template "plumber/command/shape/struct" $scope -}}
    {{ end }}
{{ end }}
{{end}}
