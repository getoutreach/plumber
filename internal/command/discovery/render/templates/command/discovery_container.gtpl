 {{ define "plumber/command/discovery/container" }}
    {{ module_import "context" -}}
    // {{ .Scope.Container.Name }} dependency container
    type {{ .Scope.Container.Name }} struct {}

    // Define dependency resolvers
    func (c *{{ .Scope.Container.Name }}) Define(ctx context.Context, cf {{ type .Scope.Config }}, a *Container) {
    }
{{ end }}
