{{define "plumber/command/shape"}}
{{ $name := or (expand_name (annotation_value $.Scope.Subject "plumber:name") .Type) .Type.Name }}
{{ with $scope := extend $ "Name" $name -}}
    {{ if $.Type.Interface -}}
        {{ template "plumber/command/shape/interface" $scope -}}
    {{ end }}
    {{ if $.Type.Struct -}}
        {{ template "plumber/command/shape/struct" $scope -}}
    {{ end }}
{{ end }}
{{end}}
