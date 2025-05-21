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
  @spec to_struct(t(), [String.t()], Keyword.t()) :: struct()
  def to_struct(%__MODULE__{} = schema, row, opts \\ []) do
    # Start with an empty struct of the module type
    struct = struct(schema.module)

    # Check if headers were provided in options
    headers = Keyword.get(opts, :headers)

    # Process regular fields with position-based or header-based mapping
    struct_with_fields = process_fields(schema, row, headers, struct, nil, opts)

    # Process embedded fields
    struct_with_embeds = process_embeds(schema, row, headers, struct_with_fields, nil, opts)

    struct_with_embeds
  end

  @doc """
  Converts a row of data to a struct based on the schema, using headers for field mapping.

  ## Parameters

    * `schema` - The schema definition
    * `row` - A list of field values 
    * `headers` - A list of header strings matching the row fields
    * `opts` - Additional options for processing

  ## Returns

    * A struct based on the schema with field values
  """
  @spec to_struct_with_headers(t(), [String.t()], [String.t()], Keyword.t()) :: struct()
  def to_struct_with_headers(%__MODULE__{} = schema, row, headers, opts \\ []) do
    # Pass the headers along in the options
    to_struct(schema, row, Keyword.put(opts, :headers, headers))
  end

  # This function was removed as we no longer use header-based mapping

  # Process regular fields
  defp process_fields(%__MODULE__{} = schema, row, headers, struct, _header_positions, opts) do
    # Get non-embed fields
    regular_fields = Enum.filter(schema.fields, fn field -> field.type != :embed end)

    # Process each field
    regular_fields
    |> Enum.with_index()
    |> Enum.reduce(struct, fn {field, idx}, acc ->
      # Use header-based mapping if headers are provided, otherwise use position-based
      col_idx =
        if is_list(headers) do
          # Find the header by field name or label
          field_label = field.opts[:label] || Atom.to_string(field.name)

          Enum.find_index(headers, fn header ->
            String.downcase(header) == String.downcase(field_label)
          end)
        else
          # Use index-based mapping (positional) - this assumes fields are defined in the same order as columns
          idx
        end

      # Get the raw value from the row if column was found
      raw_value =
        if is_nil(col_idx) || col_idx >= length(row), do: nil, else: Enum.at(row, col_idx)

      # Handle empty fields appropriately
      raw_value = if raw_value == "", do: nil, else: raw_value

      # Apply default value for nil fields
      raw_value =
        if is_nil(raw_value) && Keyword.has_key?(field.opts, :default),
          do: Keyword.get(field.opts, :default),
          else: raw_value

      # Pass trim_fields option to the field
      field_with_opts = %{
        field
        | opts: Keyword.merge(field.opts, Keyword.take(opts, [:trim_fields]))
      }

      # Parse the value according to field type
      parsed_value = Field.parse_value(raw_value, field_with_opts)

      # Add to accumulator
      Map.put(acc, field.name, parsed_value)
    end)
  end

  # Process embedded fields
  defp process_embeds(%__MODULE__{} = schema, row, headers, struct, _header_positions, opts) do
    # Get embed fields
    embed_fields = get_embeds(schema)

    # Process each embed
    Enum.reduce(embed_fields, struct, fn field, acc ->
      # Get the module for this embed
      embed_module = schema.embeds[field.name]
      embed_schema = embed_module.__delimit_schema__()
      # Get the prefix for this embed's fields
      prefix = get_embed_prefix(field)

      # Build a struct for this embed, passing along options
      embed_struct = to_struct_with_prefix(embed_schema, row, headers, prefix, nil, opts)

      # Add to accumulator
      Map.put(acc, field.name, embed_struct)
    end)
  end

  # This function was removed as we no longer use header-based mapping

  defp to_struct_with_prefix(
         %__MODULE__{} = schema,
         row,
         headers,
         prefix,
         _header_positions,
         opts
       ) do
    # Start with an empty struct
    struct = struct(schema.module)

    # Get non-embed fields from the embedded schema
    regular_fields = Enum.filter(schema.fields, fn field -> field.type != :embed end)

    # Map over each field in the embedded schema
    Enum.reduce(regular_fields, struct, fn field, acc ->
      # For each field, look for a column with prefix + field name
      field_name = Atom.to_string(field.name)
      field_label = field.opts[:label] || field_name
      prefixed_field = prefix <> field_label

      # Find the position of this field in the row using the prefix
      # If headers are available, use header-based mapping
      col_idx =
        if is_list(headers) do
          # Find the column index by looking for the prefixed field name in headers
          Enum.find_index(headers, fn header ->
            String.trim(String.downcase(header)) ==
              String.trim(String.downcase(prefixed_field))
          end)

          # No headers available, can't do position-based mapping for embeds
        end

      # Get the raw value from the row if column was found
      raw_value =
        if is_nil(col_idx) || col_idx >= length(row), do: nil, else: Enum.at(row, col_idx)

      # Handle empty fields appropriately
      raw_value = if raw_value == "", do: nil, else: raw_value

      # Apply default value for nil fields
      raw_value =
        if is_nil(raw_value) && Keyword.has_key?(field.opts, :default),
          do: Keyword.get(field.opts, :default),
          else: raw_value

      # Pass trim_fields option to the field
      field_with_opts = %{
        field
        | opts: Keyword.merge(field.opts, Keyword.take(opts, [:trim_fields]))
      }

      # Parse the value according to field type
      parsed_value = Field.parse_value(raw_value, field_with_opts)

      # Debug statements removed for production
      # IO.puts("Field #{field.name} parsed value: #{inspect(parsed_value)}")

      # Add to accumulator
      Map.put(acc, field.name, parsed_value)
    end)
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
    # Debug statements removed for production
    # IO.inspect(struct_or_map, label: "Converting struct to row")

    # Get regular fields (no embeds)
    regular_fields = Enum.filter(schema.fields, fn field -> field.type != :embed end)

    # Get all embedded fields
    embed_fields = get_embeds(schema)

    # Start with regular field values
    row_values =
      Enum.map(regular_fields, fn field ->
        value = Map.get(struct_or_map, field.name)
        Field.to_string(value, field)
      end)

    # Add embedded field values
    embedded_values =
      Enum.flat_map(embed_fields, fn embed_field ->
        embed_struct = Map.get(struct_or_map, embed_field.name)

        if is_nil(embed_struct) do
          # If the embedded struct is nil, add empty values for all its fields
          embed_module = schema.embeds[embed_field.name]
          embed_schema = embed_module.__delimit_schema__()
          embed_fields = Enum.filter(embed_schema.fields, fn f -> f.type != :embed end)
          List.duplicate("", length(embed_fields))
        else
          # Get the embedded schema and its fields
          embed_module = schema.embeds[embed_field.name]
          embed_schema = embed_module.__delimit_schema__()
          embed_fields = Enum.filter(embed_schema.fields, fn f -> f.type != :embed end)

          # Debug statements removed for production
          # IO.inspect(embed_struct, label: "Embedded struct #{embed_field.name}")
          # IO.inspect(embed_fields, label: "Embedded fields in #{embed_field.name}")

          # Convert each embedded field to a string
          embedded_values =
            Enum.map(embed_fields, fn field ->
              value = Map.get(embed_struct, field.name)
              string_value = Field.to_string(value, field)
              # IO.puts("Field #{field.name} = #{inspect(value)} -> #{string_value}")
              string_value
            end)

          embedded_values
        end
      end)

    # Combine regular and embedded values
    result = row_values ++ embedded_values
    # IO.inspect(result, label: "Generated row")
    result
  end

  # Note: These header-related functions have been removed as we're no longer using header-based mapping

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

    # Combine regular and embedded headers
    regular_headers ++ embed_headers
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

      :time ->
        quote do: Time.t()

      :naive_datetime ->
        quote do: NaiveDateTime.t()

      :embed ->
        quote do: struct()

      {:list, inner_type} ->
        inner_typespec = type_to_typespec(inner_type)
        quote do: list(unquote(inner_typespec))

      {:map, key_type, value_type} ->
        key_typespec = type_to_typespec(key_type)
        value_typespec = type_to_typespec(value_type)
        quote do: %{optional(unquote(key_typespec)) => unquote(value_typespec)}

      {:map, value_type} ->
        value_typespec = type_to_typespec(value_type)
        quote do: %{optional(atom()) => unquote(value_typespec)}

      _other ->
        quote do: any()
    end
  end
end
