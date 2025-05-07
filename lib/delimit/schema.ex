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
  """
  @type schema_options :: [
          headers: boolean(),
          delimiter: String.t(),
          line_ending: String.t(),
          skip_lines: non_neg_integer(),
          skip_while: (String.t() -> boolean()),
          trim_fields: boolean(),
          nil_on_empty: boolean()
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
    
    # Process each field and build the struct
    schema.fields
    |> Enum.with_index()
    |> Enum.reduce(struct, fn {field, idx}, acc ->
      # Find the column index if headers are provided
      col_idx = if headers do
        header_name = Atom.to_string(field.name)
        Enum.find_index(headers, fn h -> h == header_name end) || idx
      else
        idx
      end
      
      # Get the raw value from the row
      raw_value = Enum.at(row, col_idx)
      
      # Parse the value according to field type
      parsed_value = Field.parse_value(raw_value, field)
      
      # Add to accumulator
      Map.put(acc, field.name, parsed_value)
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
    # Determine the field order
    fields = if headers do
      # Map headers to fields
      Enum.map(headers, fn header ->
        field_name = String.to_existing_atom(header)
        get_field(schema, field_name)
      end)
      |> Enum.filter(&(&1 != nil))
    else
      schema.fields
    end
    
    # Convert each field to string
    Enum.map(fields, fn field ->
      value = Map.get(struct_or_map, field.name)
      Field.to_string(value, field)
    end)
  end
  
  @doc """
  Gets the headers for the schema.

  ## Parameters

    * `schema` - The schema definition

  ## Returns

    * List of header names as strings

  ## Example

      iex> schema = Delimit.Schema.new(MyApp.Person)
      iex> schema = Delimit.Schema.add_field(schema, :first_name, :string)
      iex> schema = Delimit.Schema.add_field(schema, :last_name, :string)
      iex> Delimit.Schema.headers(schema)
      ["first_name", "last_name"]
  """
  @spec headers(t()) :: [String.t()]
  def headers(%__MODULE__{} = schema) do
    schema
    |> field_names()
    |> Enum.map(&Atom.to_string/1)
  end
end