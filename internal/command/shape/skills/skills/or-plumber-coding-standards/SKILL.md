---
name: outreach-coding-standards
description: Provides guidance on coding standards, best practices, and conventions for writing clean, maintainable, and efficient Go code.
---

# Coding Standards

## Comments

- Use comments to explain the "why" behind code, not the "what". The code should be self-explanatory about what it does; comments should provide context, rationale, and intent.
- Use complete sentences in comments, starting with a capital letter and ending with a period.
- For package-level comments, provide an overview of the package's purpose and functionality.
- For function comments, describe the function's behavior, parameters, return values, and any side effects.
- Limit comments to 140 characters per line for readability.

## Error Handling

- When asserting errors, use `errors.Is` or `errors.As` to check for specific error types instead of comparing error strings like ` ve, ok := err.(*jsonschema.ValidationError);`.

## Naming Conventions
- Avoid name shuttering with package names. For example, if the package is named `validate` avoid naming a function `ValidateUser` that would require calling it as `validate.ValidateUser()`. Instead, choose a simpler name like `User` that can be called as `validate.User()`, which is more concise and readable.
- It is ok to have some name shuttering when it improves clarity, such as `config.Config`, where the package name provides important context about the type being defined.
