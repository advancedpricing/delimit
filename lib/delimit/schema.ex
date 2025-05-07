defmodule Delimit.Schema do
  @moduledoc """
  Defines the schema structure and operations for delimited data.
  
  This module handles schema definitions including fields and embedded schemas.
  It provides functionality for managing the structure of delimited data.
  """

  alias Delimit.Field

  @typedoc """
  Schema definition structure.

  * `:module` - The module where this schema is defined
  * `:fields` - List of field definitions
  * `:embeds` - Map of embedded schemas
  * `:options` - Global options for the schema
  """
  @type t :: %__MODULE__{
          module: module(),
          fields: [Field.t()],
          embeds: %{atom() => module()},
          options: Keyword.t()
        }

  defstruct [:module, fields: [], embeds: %{}, options: []]

  @typedoc """
  Schema options that control reading and writing behavior.

  * `:headers` - Whether the file includes headers (default: true)
  * `:delimiter` - The delimiter character (default: comma)
  * `:line_ending` - Line ending to use (default: platform-specific)
  * `:skip_lines` - Number of lines to skip at the beginning (default: 0)
  * `:skip_while` - Function that returns true for lines to skip
  * `:trim_fields` - Whether to trim whitespace from fields (default: true)
  * `:nil_on_empty` - Convert empty strings to nil (default: true)
  * `:prefix` - Prefix to add to field headers for embedded schemas
  """
  @type schema_options :: [
          headers: boolean(),
          delimiter: String.t(),
          line_ending: String.t(),
          skip_lines: non_neg_integer(),
          skip_while: (String.t() -> boolean()),
          trim_fields: boolean(),
          nil_on_empty: boolean(),
          prefix: String.t()
        ]

  @doc """
  Creates a new schema definition.

  ## Parameters

    * `module` - The module where the schema is defined
    * `options` - Global options for the schema

  ## Example

      iex> Delimit.Schema.new(MyApp.Person, headers: true, delimiter: ",")
      %Delimit.Schema{module: MyApp.Person, fields: [], embeds: %{}, options: [headers: true, delimiter: ","]}
  """
  @spec new(module(), schema_options()) :: t()
  def new(module, options \\ []) when is_atom(module) do
    %__MODULE__{
      module: module,
      options: options
    }
  end

  @doc """
  Adds a field to the schema.

  ## Parameters

    * `schema` - The schema to add the field to
    * `name` - Field name as atom
    * `type` - Field type
    * `opts` - Field options

  ## Returns

    * Updated schema with the new field

  ## Example

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :name, :string)
      iex> schema = Delimit.Schema.add_field(schema, :age, :integer, default: 0)
      iex> length(schema.fields)
      2
  """
  @spec add_field(t(), atom(), Field.field_type(), Field.field_opts()) :: t()
  def add_field(%__MODULE__{} = schema, name, type, opts \\ []) do
    field = Field.new(name, type, opts)
    %{schema | fields: schema.fields ++ [field]}
  end

  @doc """
  Adds an embedded schema to the parent schema.

  ## Parameters

    * `schema` - The parent schema
    * `name` - Name for the embedded schema as atom
    * `module` - Module that defines the embedded schema
    * `opts` - Options for the embedded schema relationship

  ## Returns

    * Updated schema with the embedded schema reference

  ## Example

      iex> schema = Delimit.Schema.new(MyApp.Invoice)
      iex> schema = Delimit.Schema.add_embed(schema, :billing_address, MyApp.Address)
      iex> Map.has_key?(schema.embeds, :billing_address)
      true
  """
  @spec add_embed(t(), atom(), module(), Keyword.t()) :: t()
  def add_embed(%__MODULE__{} = schema, name, module, opts \\ []) when is_atom(name) and is_atom(module) do
    # Add a special field to indicate an embed
    field = Field.new(name, :embed, opts)
    %{schema | 
      fields: schema.fields ++ [field], 
      embeds: Map.put(schema.embeds, name, module)
    }
  end

  @doc """
  Gets field names in order of definition.

  ## Parameters

    * `schema` - The schema

  ## Returns

    * List of field names as atoms

  ## Example

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :first_name, :string)
      iex> schema = Delimit.Schema.add_field(schema, :last_name, :string)
      iex> Delimit.Schema.field_names(schema)
      [:first_name, :last_name]
  """
  @spec field_names(t()) :: [atom()]
  def field_names(%__MODULE__{fields: fields}) do
    Enum.map(fields, fn %Field{name: name} -> name end)
  end

  @doc """
  Gets a field by name.

  ## Parameters

    * `schema` - The schema
    * `name` - Field name to find

  ## Returns

    * Field struct or nil if not found

  ## Example

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :name, :string)
      iex> field = Delimit.Schema.get_field(schema, :name)
      iex> field.type
      :string
  """
  @spec get_field(t(), atom()) :: Field.t() | nil
  def get_field(%__MODULE__{fields: fields}, name) when is_atom(name) do
    Enum.find(fields, fn %Field{name: field_name} -> field_name == name end)
  end

  @doc """
  Gets all embedded fields defined in the schema.

  ## Parameters

    * `schema` - The schema to check

  ## Returns

    * List of fields of type :embed

  ## Example

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_embed(schema, :address, Address)
      iex> [embed] = Delimit.Schema.get_embeds(schema)
      iex> embed.name
      :address
  """
  @spec get_embeds(t()) :: [Field.t()]
  def get_embeds(%__MODULE__{fields: fields}) do
    Enum.filter(fields, fn %Field{type: type} -> type == :embed end)
  end

  @doc """
  Gets the header prefix for an embedded field.

  ## Parameters

    * `field` - The embed field
    * `default_prefix` - Default prefix to use if none specified

  ## Returns

    * String prefix to use for field headers
  """
  @spec get_embed_prefix(Field.t(), String.t() | nil) :: String.t()
  def get_embed_prefix(%Field{name: name, opts: opts}, default_prefix \\ nil) do
    # Use explicit prefix in options, or generate from field name
    prefix = opts[:prefix] || if default_prefix, do: default_prefix, else: "#{name}_"
    
    # Ensure prefix ends with underscore
    if String.ends_with?(prefix, "_"), do: prefix, else: prefix <> "_"
  end

  @doc """
  Creates a struct from row data based on the schema.

  ## Parameters

    * `schema` - The schema definition
    * `row` - Row data as list of strings
    * `headers` - Optional list of headers to map fields

  ## Returns

    * Struct with parsed data based on schema

  ## Example

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :name, :string)
      iex> schema = Delimit.Schema.add_field(schema, :age, :integer)
      iex> Delimit.Schema.to_struct(schema, ["John Doe", "42"])
      %{name: "John Doe", age: 42}
  """
  @spec to_struct(t(), [String.t()], [String.t()] | nil) :: struct() | map()
  def to_struct(%__MODULE__{} = schema, row, headers \\ nil) do
    # Start with an empty map
    struct = %{}
    
    # Process regular fields
    struct_with_fields = process_fields(schema, row, headers, struct)
    
    # Process embedded fields
    struct_with_embeds = process_embeds(schema, row, headers, struct_with_fields)
    
    struct_with_embeds
  end
  
  # Process regular fields
  defp process_fields(%__MODULE__{} = schema, row, headers, struct) do
    # Get non-embed fields
    regular_fields = Enum.filter(schema.fields, fn field -> field.type != :embed end)
    
    # Process each field
    regular_fields
    |> Enum.with_index()
    |> Enum.reduce(struct, fn {field, idx}, acc ->
      # Find the column index if headers are provided
      col_idx = if headers do
        # For headers, we may have label or prefix, so need to try different possibilities
        header_name = field.opts[:label] || Atom.to_string(field.name)
        Enum.find_index(headers, fn h -> h == header_name end) || idx
      else
        idx
      end
      
      # Get the raw value from the row if column was found
      raw_value = if is_nil(col_idx) || col_idx >= length(row), do: nil, else: Enum.at(row, col_idx)
      
      # Parse the value according to field type
      parsed_value = Field.parse_value(raw_value, field)
      
      # Add to accumulator
      Map.put(acc, field.name, parsed_value)
    end)
  end
  
  # Process embedded fields
  defp process_embeds(%__MODULE__{} = schema, row, headers, struct) do
    # Get all embed fields
    embed_fields = get_embeds(schema)
    
    # Process each embed
    Enum.reduce(embed_fields, struct, fn field, acc ->
      # Get the module for this embed
      embed_module = schema.embeds[field.name]
      
      # Get the embed schema
      embed_schema = embed_module.__delimit_schema__()
      
      # Get the prefix for this embed's fields
      prefix = get_embed_prefix(field)
      
      # Create a struct for this embed
      embed_struct = to_struct_with_prefix(embed_schema, row, headers, prefix)
      
      # Add to accumulator
      Map.put(acc, field.name, embed_struct)
    end)
  end
  
  # Create a struct with prefixed headers
  defp to_struct_with_prefix(%__MODULE__{} = schema, row, headers, prefix) do
    # Start with an empty map
    struct = %{}
    
    # Process each field in the embed
    schema.fields
    |> Enum.reduce(struct, fn field, acc ->
      # Find column index if headers provided
      col_idx = if headers do
        # Try with prefix + label or prefix + name
        header_name = prefix <> (field.opts[:label] || Atom.to_string(field.name))
        Enum.find_index(headers, fn h -> h == header_name end)
      end
      
      # Get raw value and parse, or use default
      if !is_nil(col_idx) && col_idx < length(row) do
        # Get the raw value from the row
        raw_value = Enum.at(row, col_idx)
        
        # Parse the value according to field type
        parsed_value = Field.parse_value(raw_value, field)
        
        # Add to accumulator
        Map.put(acc, field.name, parsed_value)
      else
        # Column not found, use default value
        default = field.opts[:default]
        if default, do: Map.put(acc, field.name, default), else: acc
      end
    end)
  end
  
  @doc """
  Converts a struct or map to a row of string values based on the schema.

  ## Parameters

    * `schema` - The schema definition
    * `struct` - Struct or map containing the data
    * `headers` - Optional list of headers to determine field order

  ## Returns

    * List of string values in the order defined by the schema or headers

  ## Example

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :name, :string)
      iex> schema = Delimit.Schema.add_field(schema, :age, :integer)
      iex> Delimit.Schema.to_row(schema, %{name: "John Doe", age: 42})
      ["John Doe", "42"]
  """
  @spec to_row(t(), struct() | map(), [String.t()] | nil) :: [String.t()]
  def to_row(%__MODULE__{} = schema, struct_or_map, headers \\ nil) do
    if headers do
      # With headers, output fields in header order
      to_row_with_headers(schema, struct_or_map, headers)
    else
      # Without headers, use schema field order
      to_row_from_schema(schema, struct_or_map)
    end
  end
  
  # Convert to row using headers for ordering
  defp to_row_with_headers(%__MODULE__{} = schema, struct_or_map, headers) do
    # Create a map of field values keyed by their headers
    field_values = build_field_value_map(schema, struct_or_map)
    
    # Output values in header order
    Enum.map(headers, fn header ->
      Map.get(field_values, header, "")
    end)
  end
  
  # Convert to row using schema field ordering
  defp to_row_from_schema(%__MODULE__{} = schema, struct_or_map) do
    # Process regular fields
    regular_fields = Enum.filter(schema.fields, fn field -> field.type != :embed end)
    regular_values = Enum.map(regular_fields, fn field ->
      value = Map.get(struct_or_map, field.name)
      Field.to_string(value, field)
    end)
    
    # Process embedded fields
    embed_fields = get_embeds(schema)
    embed_values = Enum.flat_map(embed_fields, fn field ->
      embed_value = Map.get(struct_or_map, field.name)
      embed_module = schema.embeds[field.name]
      embed_schema = embed_module.__delimit_schema__()
      
      # Get prefix for this embed
      prefix = get_embed_prefix(field)
      
      # Convert embed to row values
      to_embed_values(embed_schema, embed_value, prefix)
    end)
    
    regular_values ++ embed_values
  end
  
  # Build a map of field values keyed by their header names
  defp build_field_value_map(%__MODULE__{} = schema, struct_or_map) do
    # Process regular fields
    regular_fields = Enum.filter(schema.fields, fn field -> field.type != :embed end)
    regular_values = Enum.reduce(regular_fields, %{}, fn field, acc ->
      header = field.opts[:label] || Atom.to_string(field.name)
      value = Map.get(struct_or_map, field.name)
      string_value = Field.to_string(value, field)
      Map.put(acc, header, string_value)
    end)
    
    # Process embedded fields
    embed_fields = get_embeds(schema)
    embed_values = Enum.reduce(embed_fields, %{}, fn field, acc ->
      embed_value = Map.get(struct_or_map, field.name)
      embed_module = schema.embeds[field.name]
      embed_schema = embed_module.__delimit_schema__()
      
      # Get prefix for this embed
      prefix = get_embed_prefix(field)
      
      # Add embed values with prefixed headers
      embed_field_map = build_embed_field_value_map(embed_schema, embed_value, prefix)
      Map.merge(acc, embed_field_map)
    end)
    
    Map.merge(regular_values, embed_values)
  end
  
  # Build field value map for embedded schemas
  defp build_embed_field_value_map(%__MODULE__{} = schema, embed_value, prefix) do
    Enum.reduce(schema.fields, %{}, fn field, acc ->
      header = prefix <> (field.opts[:label] || Atom.to_string(field.name))
      value = if embed_value, do: Map.get(embed_value, field.name), else: nil
      string_value = Field.to_string(value, field)
      Map.put(acc, header, string_value)
    end)
  end
  
  # Convert embed to row values
  defp to_embed_values(%__MODULE__{} = schema, embed_value, _prefix) do
    Enum.map(schema.fields, fn field ->
      value = if embed_value, do: Map.get(embed_value, field.name), else: nil
      Field.to_string(value, field)
    end)
  end
  
  @doc """
  Gets the headers for the schema.

  ## Parameters

    * `schema` - The schema definition
    * `prefix` - Optional prefix to add to field headers

  ## Returns

    * List of header names as strings

  ## Example

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :first_name, :string)
      iex> schema = Delimit.Schema.add_field(schema, :last_name, :string)
      iex> Delimit.Schema.headers(schema)
      ["first_name", "last_name"]

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :street, :string, label: "address_street")
      iex> Delimit.Schema.headers(schema)
      ["address_street"]

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :street, :string)
      iex> Delimit.Schema.headers(schema, "billing_")
      ["billing_street"]
  """
  @spec headers(t(), String.t() | nil) :: [String.t()]
  def headers(%__MODULE__{} = schema, prefix \\ nil) do
    # Get regular field headers
    regular_headers = 
      schema.fields
      |> Enum.filter(fn field -> field.type != :embed end)
      |> Enum.map(fn field ->
        # For regular fields, use the field name or label
        base_name = field.opts[:label] || Atom.to_string(field.name)
        
        # Add prefix if provided
        if prefix do prefix <> base_name else base_name end
      end)
    
    # Get headers from embedded schemas
    embed_headers = 
      schema.fields
      |> Enum.filter(fn field -> field.type == :embed end)
      |> Enum.flat_map(fn field ->
        # For embedded fields, get the module
        embed_module = schema.embeds[field.name]
        embed_schema = embed_module.__delimit_schema__()
        
        # Get the prefix for this embed
        field_prefix = get_embed_prefix(field, prefix)
        
        # Get the headers for the embed with the combined prefix
        headers(embed_schema, field_prefix)
      end)
    
    # Combine regular and embedded headers
    regular_headers ++ embed_headers
  end
end