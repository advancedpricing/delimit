defmodule Delimit.Schema do
  @moduledoc """
  Defines schema structures and functions for working with delimited data.

  This module handles schema definitions, data type conversions, and transformations
  between delimited data and Elixir structs.
  """

  alias Delimit.Field

  @typedoc """
  Schema definition structure.

  * `:module` - The module associated with the schema
  * `:fields` - List of field definitions
  * `:options` - Additional options for the schema
  * `:embeds` - Map of module references for embedded schemas
  """
  @type t :: %__MODULE__{
          module: module(),
          fields: [Field.t()],
          options: schema_options(),
          embeds: %{atom() => module()}
        }

  defstruct module: nil,
            fields: [],
            options: [],
            embeds: %{}

  @typedoc """
  Options for schema handling.

  * `:delimiter` - Field delimiter character (default: comma)
  * `:skip_lines` - Number of lines to skip at beginning of file
  * `:skip_while` - Function to determine which lines to skip
  * `:trim_fields` - Whether to trim whitespace from fields (default: true)
  * `:nil_on_empty` - Convert empty strings to nil (default: true)
  * `:line_ending` - Line ending character(s) for output
  * `:format` - Predefined format (`:csv`, `:tsv`, `:psv`) that sets appropriate options
  """
  @type schema_options :: [
          delimiter: String.t(),
          skip_lines: non_neg_integer(),
          skip_while: (String.t() -> boolean()),
          trim_fields: boolean(),
          nil_on_empty: boolean(),
          line_ending: String.t(),
          format: atom()
        ]

  @doc """
  Creates a new schema definition.

  ## Parameters

    * `module` - The module associated with the schema
    * `options` - Options for the schema

  ## Returns

    * A new schema structure
  """
  @spec new(module(), schema_options()) :: t()
  def new(module, options \\ []) do
    %__MODULE__{
      module: module,
      fields: [],
      options: options,
      embeds: %{}
    }
  end

  @doc """
  Adds a field to the schema.

  ## Parameters

    * `schema` - The schema to add the field to
    * `name` - The name of the field as an atom
    * `type` - The type of the field (:string, :integer, etc.)
    * `opts` - Options for the field

  ## Returns

    * Updated schema structure
  """
  @spec add_field(t(), atom(), atom(), Keyword.t()) :: t()
  def add_field(%__MODULE__{} = schema, name, type, opts \\ []) do
    field = Field.new(name, type, opts)
    %{schema | fields: schema.fields ++ [field]}
  end

  @doc """
  Adds an embedded schema to the parent schema.

  ## Parameters

    * `schema` - The parent schema to add the embedded schema to
    * `name` - The name for the embedded schema as an atom
    * `module` - The module defining the embedded schema
    * `opts` - Options for the embedded schema

  ## Returns

    * Updated schema structure
  """
  @spec add_embed(t(), atom(), module(), Keyword.t()) :: t()
  def add_embed(%__MODULE__{} = schema, name, module, opts \\ []) do
    # Create a field for this embed
    embed_field = Field.new(name, :embed, opts)

    # Add to fields list and update embeds map
    %{
      schema
      | fields: schema.fields ++ [embed_field],
        embeds: Map.put(schema.embeds, name, module)
    }
  end

  @doc """
  Gets field names in order of definition.

  ## Parameters

    * `schema` - The schema definition

  ## Returns

    * List of field names as atoms
  """
  @spec field_names(t()) :: [atom()]
  def field_names(%__MODULE__{fields: fields}) do
    Enum.map(fields, fn field -> field.name end)
  end

  @doc """
  Gets a field by name.

  ## Parameters

    * `schema` - The schema definition
    * `name` - The field name to find

  ## Returns

    * The field definition or nil if not found
  """
  @spec get_field(t(), atom()) :: Field.t() | nil
  def get_field(%__MODULE__{fields: fields}, name) do
    Enum.find(fields, fn field -> field.name == name end)
  end

  @doc """
  Gets all embedded fields defined in the schema.

  ## Parameters

    * `schema` - The schema definition

  ## Returns

    * List of embedded field definitions
  """
  @spec get_embeds(t()) :: [Field.t()]
  def get_embeds(%__MODULE__{fields: fields}) do
    Enum.filter(fields, fn field -> field.type == :embed end)
  end

  @doc """
  Gets the header prefix for an embedded field.

  ## Parameters

    * `field` - The embedded field definition
    * `default_prefix` - Default prefix to use if none specified

  ## Returns

    * String prefix to use for field headers
  """
  @spec get_embed_prefix(Field.t(), String.t() | nil) :: String.t()
  def get_embed_prefix(%Field{name: name, opts: opts}, default_prefix \\ nil) do
    # Use specified prefix, or name + underscore
    case Keyword.get(opts, :prefix) do
      nil ->
        default_prefix || "#{name}_"

      prefix ->
        # Ensure prefix ends with underscore if not empty
        if prefix != "" and not String.ends_with?(prefix, "_"), do: prefix <> "_", else: prefix
    end
  end

  @doc """
  Converts a row of data to a struct based on the schema.

  ## Parameters

    * `schema` - The schema definition
    * `row` - A list of field values or a map of field name/values

  ## Returns

    * A struct based on the schema with field values

  ## Example

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :name, :string)
      iex> schema = Delimit.Schema.add_field(schema, :age, :integer)
      iex> Delimit.Schema.to_struct(schema, ["John Doe", "42"])
      %MyApp.Person{name: "John Doe", age: 42}
  """
  @spec to_struct(t(), [String.t()]) :: struct()
  def to_struct(%__MODULE__{} = schema, row) do
    # Start with an empty struct of the module type
    struct = struct(schema.module)

    # Process regular fields with position-based mapping
    struct_with_fields = process_fields(schema, row, nil, struct, nil)

    # Process embedded fields
    struct_with_embeds = process_embeds(schema, row, nil, struct_with_fields, nil)

    struct_with_embeds
  end
  
  # This function was removed as we no longer use header-based mapping

  # Process regular fields
  defp process_fields(%__MODULE__{} = schema, row, _headers, struct, _header_positions) do
    # Get non-embed fields
    regular_fields = Enum.filter(schema.fields, fn field -> field.type != :embed end)

    # Process each field
    regular_fields
    |> Enum.with_index()
    |> Enum.reduce(struct, fn {field, idx}, acc ->
      # Use index-based mapping (positional)
      col_idx = idx

      # Get the raw value from the row if column was found
      raw_value =
        if is_nil(col_idx) || col_idx >= length(row), do: nil, else: Enum.at(row, col_idx)

      # Handle empty fields appropriately
      raw_value = if raw_value == "", do: nil, else: raw_value

      # Parse the value according to field type if not nil
      parsed_value = if is_nil(raw_value), do: nil, else: Field.parse_value(raw_value, field)

      # Add to accumulator
      Map.put(acc, field.name, parsed_value)
    end)
  end

  # Process embedded fields
  defp process_embeds(%__MODULE__{} = schema, row, _headers, struct, _embed_pos) do
    # Get embed fields
    embed_fields = get_embeds(schema)

    # Process each embed
    Enum.reduce(embed_fields, struct, fn field, acc ->
      # Get the module for this embed
      embed_module = schema.embeds[field.name]
      embed_schema = embed_module.__delimit_schema__()
      # Get the prefix for this embed's fields
      prefix = get_embed_prefix(field)
        
      # Build a struct for this embed
      embed_struct = to_struct_with_prefix(embed_schema, row, nil, prefix, nil)
      # Add to accumulator
      Map.put(acc, field.name, embed_struct)
    end)
  end
  
  # This function was removed as we no longer use header-based mapping

  defp to_struct_with_prefix(%__MODULE__{} = schema, _row, _headers, _prefix, _pos) do
    # Start with an empty struct
    struct = struct(schema.module)
    
    # For now, return an empty struct since we don't have a good way
    # to find embedded values without headers
    struct
  end

  @doc """
  Converts a struct or map to a row of values based on the schema.

  ## Parameters

    * `schema` - The schema definition
    * `struct_or_map` - A struct or map containing field values

  ## Returns

    * A list of field values

  ## Examples

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :name, :string)
      iex> Delimit.Schema.to_row(schema, %{name: "John Doe"})
      ["John Doe"]
  """
  @spec to_row(t(), struct() | map()) :: [String.t()]
  def to_row(%__MODULE__{} = schema, struct_or_map) do
    to_row_from_schema(schema, struct_or_map)
  end

  # This function was removed as we no longer use header-based mapping

  # Convert using schema field order
  defp to_row_from_schema(%__MODULE__{} = schema, struct_or_map) do
    # Get regular fields (no embeds)
    regular_fields = Enum.filter(schema.fields, fn field -> field.type != :embed end)

    # Convert each field to a string
    regular_fields
    |> Enum.map(fn field ->
      # Get the value from the struct or map
      value = Map.get(struct_or_map, field.name)

      # Convert to string using field type
      Field.to_string(value, field)
    end)
    |> Enum.concat(embed_row_values(schema, struct_or_map))
  end

  # Note: These header-related functions have been removed as we're no longer using header-based mapping

  # Get embed row values
  defp embed_row_values(%__MODULE__{} = schema, struct_or_map) do
    embed_fields = get_embeds(schema)

    Enum.flat_map(embed_fields, fn field ->
      # Get the value of the embed from the parent struct/map
      embed_value = Map.get(struct_or_map, field.name)
      # Skip if nil
      if is_nil(embed_value) do
        []
      else
        # Get the module for this embed
        embed_module = schema.embeds[field.name]
        embed_schema = embed_module.__delimit_schema__()
        # Convert to row values
        to_row_from_schema(embed_schema, embed_value)
      end
    end)
  end

  @doc """
  Gets the headers for the schema.

  ## Parameters

    * `schema` - The schema definition
    * `prefix` - Optional prefix to apply to all headers

  ## Returns

    * List of header strings

  ## Example

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :name, :string)
      iex> schema = Delimit.Schema.add_field(schema, :age, :integer)
      iex> Delimit.Schema.headers(schema)
      ["name", "age"]
      
      iex> Delimit.Schema.headers(schema, "person_")
      ["person_name", "person_age"]
  """
  @spec headers(t(), String.t() | nil) :: [String.t()]
  def headers(%__MODULE__{} = schema, prefix \\ nil) do
    # Get regular field headers
    regular_headers =
      schema.fields
      |> Enum.filter(fn field -> field.type != :embed end)
      |> Enum.map(fn field ->
        # For regular fields, use the field name or label
        header = field.opts[:label] || Atom.to_string(field.name)

        # Add prefix if provided
        if prefix, do: prefix <> header, else: header
      end)

    # Get headers from embedded schemas
    embed_headers =
      schema
      |> get_embeds()
      |> Enum.flat_map(fn field ->
        embed_module = schema.embeds[field.name]
        embed_schema = embed_module.__delimit_schema__()

        # Determine prefix (field's own prefix + parent prefix)
        field_prefix = get_embed_prefix(field)
        combined_prefix = if prefix, do: prefix <> field_prefix, else: field_prefix

        # Get headers for this embed with combined prefix
        headers(embed_schema, combined_prefix)
      end)

    # Combine regular and embedded headers and ensure uniqueness
    (regular_headers ++ embed_headers) |> Enum.uniq()
  end

  @doc """
  Converts a field type to an Elixir typespec.

  This function is used to convert field types to proper Elixir typespecs
  for use in @type definitions.

  ## Parameters

    * `type` - The field type or a tuple with more specific type information

  ## Returns

    * An Elixir typespec expression

  ## Example

      iex> Delimit.Schema.type_to_typespec(:string)
      quote do: String.t()
      
      iex> Delimit.Schema.type_to_typespec({:list, :string})
      quote do: [String.t()]
  """
  @spec type_to_typespec(atom() | tuple()) :: Macro.t()
  def type_to_typespec(type) do
    case type do
      :string ->
        quote do: String.t()

      :integer ->
        quote do: integer()

      :float ->
        quote do: float()

      :boolean ->
        quote do: boolean()

      :date ->
        quote do: Date.t()

      :datetime ->
        quote do: DateTime.t()

      :embed ->
        quote do: struct()

      {:list, inner_type} ->
        inner_typespec = type_to_typespec(inner_type)
        quote do: [unquote(inner_typespec)]

      {:map, key_type, value_type} ->
        key_typespec = type_to_typespec(key_type)
        value_typespec = type_to_typespec(value_type)
        quote do: %{optional(unquote(key_typespec)) => unquote(value_typespec)}

      {:map, value_type} ->
        value_typespec = type_to_typespec(value_type)
        quote do: %{optional(atom()) => unquote(value_typespec)}

      other ->
        quote do: unquote(other)
    end
  end
end
