scripts/protoapi.gtpl
# Shape Functions

## Annotation Value Expansion

Template functions available during annotation value expansion.

### filename_suffixed

Append a suffix to a filename.

**Usage:** `{{ filename_suffixed "suffix" }}`

**Parameters:**

- `string`

**Returns:**

- `string`

### path_join

Join multiple path segments together using the context's path resolver.

**Usage:** `{{ path_join "segment1" "segment2" }}`

**Parameters:**

- `...string`

**Returns:**

- `string`
- `error`

### macro_defaults_name

Determine a default name for an output type based on the context's source arguments or input type information.

**Usage:** `{{ macro_defaults_name }}`

**Parameters:**

- `...string`

**Returns:**

- `string`
- `error`

## Shape Template Evaluation

Template functions available during shape template rendering.

### type_wrap

Wrap a type with additional functionality

**Usage:** `{{ type_wrap .Type "WrapperName" }}`

### ignored

Check if a name is in the list of ignored names.

**Usage:** `{{ ignored "name" }}`

### expand_name

Expand a name using the context's expansion rules.

**Usage:** `{{ expand_name "name" }}`

**Parameters:**

- `string`
- `*"github.com/getoutreach/plumber/query/model".Type`

**Returns:**

- `any`

### comment

Wrap a given text into comment lines.

**Usage:** `{{ comment "This is a long comment \n\nthat needs to be wrapped." }}`

**Parameters:**

- `any`

**Returns:**

- `string`

### filter_elements

Filter elements based on certain criteria.

**Usage:** `{{ filter_elements .Elements "criteria" }}`

**Parameters:**

- `any`
- `any`
- `...string`

**Returns:**

- `any`
- `error`

### receiver

Set the current type in the evaluation context.

**Usage:** `{{ receiver }}`

**Parameters:**

- `any`

**Returns:**

- `string`

### extend

Extend the current scope with additional variables. It is useful for passing multiple variables to a template without having to create a new
struct or map. Usually used in combination with template inclusion for readability and loops to pass loop variable into the included template.

**Usage:** `{{ with $scope := extend $ "Field1" "Value1" "Field2" "Value2" -}}
    {{template "plumber/command/shape/struct/field/method" $scope -}}
{{ end }}`

**Parameters:**

- `any`
- `...any`

**Returns:**

- `any`
- `error`

### file_description

Sets the description comment for the current file. So second code gen pass that is responsible for file header can pick it up and render it as a comment in the generated code.
Example:
```golang

// Description: This file contains generated code for MyType and its methods.

package sample
```

**Usage:** `{{ file_description "This file contains generated code for MyType and its methods." }}`

### file_package_description

Sets the description comment for the current file's package.
Example:
```golang

// Package sample contains generated code for MyType and its methods.
package sample
```

**Usage:** `{{ file_package_description "contains generated code for MyType and its methods." }}`

### comment_wrap

Render text as wrapped comments.

Example:
```golang
// This is a long comment
//
// that needs to be wrapped.
```

**Usage:** `{{ comment_wrap "This is a long comment \n\nthat needs to be wrapped." }}`

**Parameters:**

- `string`

**Returns:**

- `string`

### type

Render a type by given type specification. It takes into account the imports and aliases defined in the current context to render the type in the most concise way possible.

**Usage:** `{{ type .Type.Spec }}`

### type_set

Set the current type in the evaluation context. So the methods like type_method_definable can use it.

**Usage:** `{{ type_set "MyType" }}`

### type_method_definable

Check if a method is undefined or defined within same file as the current output.
					It requires function type_set to set the type first.

**Usage:** `{{ type_method_definable "MethodName" }}`

### placeholder

Insert a placeholder in the template so it enables editing within designated area in the output. When generating code with `inplace` mode, the placeholder is not rendered.

**Usage:** `{{ placeholder "placeholder_name" }}`

### fragment_start

Renders a start of a fragment. Fragments are similar to placeholders, but allows redefine bigger areas that might contain placeholders.

**Usage:** `{{ fragment_start "fragment_name" }}`

### fragment_end

Renderers the end of a fragment.

**Usage:** `{{ fragment_end "fragment_name" }}`

### module_import

Schedules a module for import, so in second pass it will be included in the imports section of the generated file. See module function for more details.

**Usage:** `{{ module_import "module_name" }}`

### module

Schedules a module for import, so in second pass it will be included in the imports section of the generated file.
Additionally, it returns a reference to the module that can be used as helper in rendering the module's types.

It can accept:
- absolute path like `github.com/getoutreach/module` or `context`
- relative path like `../module` . The relative path is resolved based on the current output path
- structure path like `structure:domain.entity` that resolves to the module containing the specified structure.

**Usage:** `{{ $entity      := module "structure:domain.entity" -}}
{{ $entity.Ident "TypeName" }}`

### annotation

Get the annotation with the specified name from an object.

**Usage:** `{{ annotation .Type "annotation_name" }}`

**Parameters:**

- `any`
- `string`

**Returns:**

- `*"github.com/getoutreach/plumber/query/model".Annotation`

### annotation_value

Get the value of an annotation with the specified name from an object.

**Usage:** `{{ annotation_value .Type "annotation_name" }}`

**Parameters:**

- `any`
- `string`

**Returns:**

- `string`

### fqn_mask

Derive new FQN from given and mask that will change the name of the type but keep the same package and import path.
It is useful for rendering types that are related to each other and should be placed in the same package, like filters, parameters, results etc.

For example, if you have a type `User` with FQN `github.com/getoutreach/api.User`
and you want to render a filter type for it, you can use mask `%s_Filter` to get FQN `github.com/getoutreach/api.User_Filter` for the filter type.

**Usage:** `{{ fqn_mask .Type.Spec "%s_Filter" }}`

**Parameters:**

- `"github.com/getoutreach/plumber/query/model".TypeSpec`
- `string`

**Returns:**

- `string`
- `error`

