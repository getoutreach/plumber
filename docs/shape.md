# Shaping

Plumber can be used to

```go
	// Interface documentation
	//
	// plumber:shape
	// plumber:name MyBetterStruct
	// plumber:template template.name
	// plumber:ignore patch.State
	MyInterface interface {
		Method(ctx context.Context, a int, b string) (c bool, err error)
	}
```


```go
	// Interface documentation
	//
	// plumber:derive
	// plumber:name MyBetterStruct
	// plumber:template template.name
	// plumber:ignore patch.State
	MyInterface interface {
		Method(ctx context.Context, a int, b string) (c bool, err error)
	}
```

```shell
    go run github.com/getoutreach/plumber/cmd/plumber@version shape -c plumber.shape.yaml ./...
```
