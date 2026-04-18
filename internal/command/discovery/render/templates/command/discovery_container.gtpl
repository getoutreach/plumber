 {{ define "plumber/command/discovery/container" }}
    {{ template "plumber/command/discovery/file/copyright" . }}
    // Description: {{ .container.name }} related dependencies {{ .package_name}}
    package {{ .package_name}}

    import (
      "context"
    )

    // {{ .container.name }} dependency container
    type {{ .container.name }} struct {}

    // Define dependency resolvers
    func (c *{{ .container.name }}) Define(ctx context.Context, cf {{ .config.type }}, a *Container) {
    }
{{ end }}
