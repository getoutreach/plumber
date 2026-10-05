{{define "plumber/command/derive/struct/comment"}}{{end}}
{{define "plumber/command/derive/field/comment"}}{{end}}
{{define "plumber/command/derive/field/type" -}}
{{ type_wrap $.Scope.Subject $.Scope.Field.Type.Spec $.Scope.Field }}
{{end}}
{{define "plumber/command/derive/field" -}}
{{ template "plumber/command/derive/field/comment" $ -}}
    {{ $.Scope.Field.Name }} {{ template "plumber/command/derive/field/type" $ -}}
{{end}}
{{define "plumber/command/derive/struct/fields" -}}
    {{ range $f := filter_elements $.Scope.Subject $.Type.Struct.Fields "fields" -}}
        {{ if (ignored $f.Name) -}}{{ continue -}}{{ end -}}
        {{ with $scope := extend $ "Field" $f -}}{{template "plumber/command/derive/field" $scope -}}{{- end -}}
    {{- end -}}
{{end}}

{{define "plumber/command/derive/struct/body"}}
// {{ $.Scope.Name }} is derived from {{ $.Type.Spec.FQN }}.
{{ comment $.Scope.Subject -}}
type {{ $.Scope.Name }} struct {
    {{ template "plumber/command/derive/struct/fields" $ -}}
    {{- if ne $.Scope.Mode "inplace"}}{{ placeholder "extra" $.Scope.Name }}{{end}}
}
{{end}}

{{define "plumber/command/derive"}}
{{ template "plumber/command/derive/struct/comment" . -}}
{{ $name := or (expand_name (annotation_value $.Scope.Subject "plumber:name") .Type) .Type.Name }}
{{ with $scope := extend $ "Name" $name -}}
{{ template "plumber/command/derive/struct/body" $scope -}}
{{- end -}}
{{end}}
