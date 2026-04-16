# Service Dependency Management

Simple but effective dependency management that is focused on readability. Main goal is to get rid off the repetitive error checking during the construction.

instead of following sequence:

```golang
d1, err := buildD1()
if err != nil {
    return nil, fmt.Errorf("d1 failed: %w", err)
}

d2, err := buildD2()
if err != nil {
    return nil, fmt.Errorf("d1 failed: %w", err)
}

return &Service{
	D1: d1,
	D2: d2,
}, nil

```

We do just:

```golang
a.Service.Resolver(func(r *plumber.Resolution[*Service]) {
    r.Require(&a.D1, &a.D2).Then(func() {
        r.Resolve(&Service{
            D1: a.D1.Instance(),
            D2: a.D2.Instance(),
        })
    })
})
```

This example contains just two dependencies with in real world it might gets much more messier. Also dependencies are build just once and then reused.

## Example
```golang
// a service we want to build with certain dependencies
type service struct {
    D1 int
    D2 int
}
// application dependency graph holds all application dependencies. It is recommended to structure it based on adapter types.
a := struct {
    D1      plumber.D[int]
    D2      plumber.D[int]
    Service plumber.D[*service]
}{}
// definition of the dependencies
a.D1.Const(1)
a.D2.Const(2)

// service resolver
a.Service.Resolver(func(r *plumber.Resolution[*Service]) {
    // service depends on D1 and D2 those needs gets resolved first without an error.
    r.Require(&a.D1, &a.D2).Then(func() {
        // When all good, we can construct our service
        r.Resolve(&Service{
            D1: a.D1.Instance(),
            D2: a.D2.Instance(),
        })
    })
})
v, err := a.Service.InstanceError()
assert.NilError(t, err)
assert.Equal(t, v.D1, 1)
assert.Equal(t, v.D2, 2)
```
