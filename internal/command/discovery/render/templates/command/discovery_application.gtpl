 {{ define "plumber/command/discovery/application" }}
    {{ module_include "context" -}}
    {{ module_include "github.com/getoutreach/plumber" -}}
    // Definer allows to redefine container on startup
    type Definer = func(ctx context.Context, cf {{ type .Scope.Config }}, a *Container)

    // Container represents root application dependency container
    type Container struct {
        plumber.Container
    }

    // NewApplication returns instance of the root dependency container
    func NewApplication(ctx context.Context, cf {{ type .Scope.Config }}, definers ...Definer) *Container {
        a := &Container{
        }
        return plumber.DefineContainers(ctx, cf, definers, a,
        )
    }
{{ end }}
