{{ define "plumber/command/shape/interface/method/forward" -}}
    {{template "plumber/command/shape/interface/method/call/arguments" .Scope.Method.Results -}}
    = {{ .Scope.Ident }}.{{ .Scope.Method.Name }}(
        {{template "plumber/command/shape/interface/method/call/arguments" .Scope.Method.Args }},
    )
{{end}}

{{define "plumber/command/shape/interface/method/call/arguments" -}}
    {{ range $i, $m := . -}}
    {{ if ne $i 0 }}, {{ end -}}{{ coalesce $m.Name $m.FallbackName }}
    {{- end -}}
{{end}}

{{define "plumber/command/shape/interface/struct"}}
{{end}}
{{define "plumber/command/shape/interface/initializer/body"}}
{{end}}
{{define "plumber/command/shape/interface/initializer/params"}}
{{end}}
{{define "plumber/command/shape/interface/method/body"}}
    return
{{end}}
{{define "plumber/command/shape/interface/method/params"}}
    {{ range $i, $m := $.Scope.Method.Args -}}
    {{ coalesce $m.Name $m.FallbackName }} {{ type $m.Type.Spec }},
    {{- end }}
{{end}}
{{define "plumber/command/shape/interface/method/results"}}
    {{ range $i, $m := $.Scope.Method.Results -}}
    {{ coalesce $m.Name $m.FallbackName }} {{ type $m.Type.Spec }},
    {{- end }}
{{end}}
{{define "plumber/command/shape/interface/methods"}}
{{ range $m := $.Type.Interface.Methods }}
    {{ with $scope := extend $ "Method" $m "Receiver" (receiver $.Scope.Subject) -}}
        {{ if type_method_definable $m.Name -}}
        {{template "plumber/command/shape/interface/method" $scope -}}{{- end -}}
        {{ end }}
    {{ end }}
{{ end }}

{{define "plumber/command/shape/interface/method/comment"}}
{{ end }}

{{define "plumber/command/shape/interface/method"}}
    {{ template "plumber/command/shape/interface/method/comment" $ -}}
    func ({{ $.Scope.Receiver }} *{{ $.Scope.Name }}) {{ $.Scope.Method.Name }}(
        {{ template "plumber/command/shape/interface/method/params" $ -}}
    ) (
        {{ template "plumber/command/shape/interface/method/results" $ -}}
    ) {
        {{ template "plumber/command/shape/interface/method/body" $ -}}
    }
{{end}}

{{ define "plumber/command/shape/interface" }}
{{ template "plumber/command/shape/comment/shaped" $ -}}
{{ comment $.Scope.Subject -}}
{{ fragment_start "struct" ($.Scope.Name) -}}
{{ type_set $.Scope.Name -}}
type {{ $.Scope.Name }} struct {
{{ template "plumber/command/shape/interface/struct" $ -}}
}

// New{{ $.Scope.Name }} creates a new instance of {{ $.Scope.Name }}.
func New{{ $.Scope.Name }}(
    {{ template "plumber/command/shape/interface/initializer/params" $ -}}
) *{{ $.Scope.Name }} {
	return &{{ $.Scope.Name }}{
        {{ template "plumber/command/shape/interface/initializer/body" $ -}}
    }
}
{{ fragment_end }}
{{ template "plumber/command/shape/interface/methods" $ -}}
{{end}}

