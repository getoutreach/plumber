{{define "plumber/command/shape/struct/field/methods"}}
{{ range $f := $.Type.Struct.Fields -}}
    {{ with $scope := extend $ "Field" $f "Receiver" (receiver $.Scope.Subject) -}}
        {{template "plumber/command/shape/struct/field/method" $scope -}}{{- end -}}
    {{ end }}
{{ end }}
{{define "plumber/command/shape/struct/field/method/getter"}}
    {{ $methodName := printf "Get%s" $.Scope.Field.Name -}}
    {{ if type_method_definable $methodName -}}
    // Get{{ $.Scope.Field.Name }} returns the {{ $.Scope.Field.Name }} field.
    func ({{ $.Scope.Receiver }} {{ type $.Type.Spec }}) {{$methodName  }}() {{ type $.Scope.Field.Type.Spec }} {
        return {{ $.Scope.Receiver }}.{{ $.Scope.Field.Name }}
    }
    {{ end }}
{{end}}
{{define "plumber/command/shape/struct/field/method/setter"}}
    {{ $methodName := printf "Set%s" $.Scope.Field.Name -}}
    {{ if type_method_definable $methodName -}}
    // Set{{ $.Scope.Field.Name }} sets the {{ $.Scope.Field.Name }} field.
    func ({{ $.Scope.Receiver }} {{ type $.Type.Spec }}) {{$methodName  }}(value {{ type $.Scope.Field.Type.Spec }}) {
        {{ $.Scope.Receiver }}.{{ $.Scope.Field.Name }} = value
    }
    {{ end }}
{{end}}
{{define "plumber/command/shape/struct/field/method" }}
    {{ template "plumber/command/shape/struct/field/method/getter" . }}
    {{ template "plumber/command/shape/struct/field/method/setter" . }}
{{end}}
{{ define "plumber/command/shape/struct" }}
{{ type_set $.Scope.Name -}}
{{ with $scope := extend $ "Receiver" (receiver $.Scope.Subject) -}}
    {{ template "plumber/command/shape/struct/field/methods" $scope -}}
{{ end}}
{{ end }}
