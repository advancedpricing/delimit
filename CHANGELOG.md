# Changelog

## 0.2.0 (YYYY-MM-DD)

### Breaking Changes

- **Struct-Based Implementation**: Records are now returned as proper structs rather than maps, providing better type checking, IDE integration, and documentation.
- **Type Specifications**: All schemas now have automatically generated `@type` and `@typedoc` specifications.

### New Features

- **`struct_type` Field Option**: Added a `struct_type` field option to specify the Elixir type to use in structs when it differs from the file format type. Especially useful with custom `read_fn`/`write_fn`.
- **Complex Type Support**: Added support for complex types like `{:list, :string}` or `{:map, :string, :integer}` for accurate type specifications.
- **Documentation Generation**: Schema fields now generate proper documentation in the module's `@moduledoc`.
- **Standard Formats**: Added support for predefined formats (`:csv`, `:tsv`, `:psv`) that automatically configure appropriate delimiters and escape characters.

### Improvements

- **IDE Integration**: Benefit from better code completion and type checking in IDEs like VS Code with ElixirLS.
- **Self-Documenting API**: Module documentation now includes generated field type specifications.
- **Error Messages**: Better compile-time validation of field access through structs.

## 0.1.0 (2025-05-03) -- never published

- Initial release with map-based implementation
