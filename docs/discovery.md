# Dependency discovery

Using plumber might be to verbal. The dependency discovery aims to simplify the definition process.
Using a configuration file plumber can be instructed to find providers and declare sub containers automatically.

The discovery using using a golang parser that operates on top of AST three.

```
# the application might contain several sub application modules
applications:
  - name: application
    # for each application we defined a plumber sub containers
    containers:
      - plumber.container:
          comment: "Adapter modules"
          name: "{{ module }}"
          container:
            path: ./application_{{ module }}.go
          # matchers allows mark particular structure as providers so only those are considered
          matchers:
            # matcher taking into considiration structs
            - plumber.matcher.struct:
                # that has as well a constructing function matching flowing patterns
                contructors:
                - New{{ name }}
        # loop allows to define multiple containers using
        loop:
          # path allows scanning a folder structure and declares a variables using named re named group expression
          path: ./adapter/(?P<module>\d+)/

```
