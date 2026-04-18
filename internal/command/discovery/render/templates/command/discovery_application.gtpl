 {{ define "plumber/command/discovery/application" }}
    {{ template "plumber/command/discovery/file/copyright" . }}
    // Description: Application's dependency graph root

    package {{ .package_name }}

    import (
      "context"

      "github.com/getoutreach/plumber"
    )

    // Definer allows to redefine container on startup
    type Definer = func(ctx context.Context, cf {{ .config.type }}, a *Container)

    // Container represents root application dependency container
    type Container struct {
        plumber.Container
    }

    // NewApplication returns instance of the root dependency container
    func NewApplication(ctx context.Context, cf {{ .config.type }}, definers ...Definer) *Container {
        a := &Container{
        }
        return plumber.DefineContainers(ctx, cf, definers, a,
        )
    }
{{ end }}
