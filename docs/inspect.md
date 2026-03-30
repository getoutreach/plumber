# Plumber inspect

The *inspect* command can be used to introspect packages and output structured information about packages, types and functions.

This information can be used later by other external tools.

```shell
go run ../cmd/plumber/plumber.go inspect --format json ./... | jq '.[]?.types[]? | select(.interface) | {name: .name, type: .spec.fqn}'
```

```json
{
  "name": "Closer",
  "type": "\"github.com/getoutreach/plumber/example/contract\".Closer"
}
{
  "name": "MutatorService",
  "type": "\"github.com/getoutreach/plumber/example/contract\".MutatorService"
}
{
  "name": "OpenCloser",
  "type": "\"github.com/getoutreach/plumber/example/contract\".OpenCloser"
}
{
  "name": "Repository",
  "type": "\"github.com/getoutreach/plumber/example/contract\".Repository"
}
```
