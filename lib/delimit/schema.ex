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

    # Populate derived fields (`:row_hash`, `:raw_row`) — must run last so
    # they have access to all parsed values.
    populate_derived(schema, struct_with_embeds, row)
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
    # Non-embed, non-derived fields participate in positional/header column mapping.
    # Derived fields (`:row_hash`, `:raw_row`) are populated separately, see
    # populate_derived/3.
    regular_fields =
      Enum.filter(schema.fields, fn field ->
        field.type != :embed and not Field.derived?(field)
      end)

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

  # Populate derived field values (`:row_hash`, `:raw_row`) on a parsed struct.
  # Called after regular and embed fields have been populated so the canonical
  # encoding can read their values from the struct.
  @spec populate_derived(t(), struct() | map(), [String.t()]) :: struct() | map()
  def populate_derived(%__MODULE__{} = schema, struct_or_map, row) do
    Enum.reduce(schema.fields, struct_or_map, fn field, acc ->
      case field.type do
        :row_hash ->
          algorithm = Keyword.get(field.opts, :algorithm, :sha256)
          truncate = Keyword.get(field.opts, :truncate, 16)
          hash = row_hash(schema, acc, algorithm: algorithm, truncate: truncate)
          Map.put(acc, field.name, hash)

        :raw_row ->
          Map.put(acc, field.name, row)

        :embed ->
          # Recurse into embeds. The row passed to the embed is the same row
          # — embeds with derived fields are an unusual but supported case.
          embed_module = schema.embeds[field.name]
          embed_schema = embed_module.__delimit_schema__()
          embed_value = Map.get(acc, field.name)

          if is_nil(embed_value) do
            acc
          else
            Map.put(acc, field.name, populate_derived(embed_schema, embed_value, row))
          end

        _ ->
          acc
      end
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
    # Get regular fields (no embeds, no derived). Derived fields like
    # :row_hash and :raw_row are computed from input and are never written.
    regular_fields =
      Enum.filter(schema.fields, fn field ->
        field.type != :embed and not Field.derived?(field)
      end)

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

          embed_fields =
            Enum.filter(embed_schema.fields, fn f ->
              f.type != :embed and not Field.derived?(f)
            end)

          List.duplicate("", length(embed_fields))
        else
          # Get the embedded schema and its fields
          embed_module = schema.embeds[embed_field.name]
          embed_schema = embed_module.__delimit_schema__()

          embed_fields =
            Enum.filter(embed_schema.fields, fn f ->
              f.type != :embed and not Field.derived?(f)
            end)

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
    # Get regular field headers (skip embeds and derived fields — derived
    # fields are not written to files, so they don't need column headers)
    regular_headers =
      schema.fields
      |> Enum.filter(fn field ->
        field.type != :embed and not Field.derived?(field)
      end)
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
  Default delimiter used by `canonical_string/3` and `row_hash/3`.

  ASCII Unit Separator (0x1F) — chosen because it is highly unlikely to
  appear in real-world delimited file content, so the canonical encoding
  remains unambiguous regardless of the file's actual delimiter.
  """
  @canonical_delimiter <<0x1F>>
  def canonical_delimiter, do: @canonical_delimiter

  @doc """
  Returns a stable string encoding of a struct based on its schema.

  The encoding is deterministic for a given schema and struct content:

    * Fields appear in schema definition order.
    * Each field's value is encoded as it would be written to a file
      (using configured `format:` / `formats:` / `write_fn`, etc.).
    * `nil` values encode as the empty string.
    * Embedded schemas contribute their own canonical encoding recursively
      (in their declared schema order, no prefix).
    * Derived field types (`:row_hash`, `:raw_row`) are skipped — their
      values come from the parsed source row, not from canonical state.

  ## Options

    * `:delimiter` — the separator between encoded field values.
      Defaults to `Delimit.Schema.canonical_delimiter/0`
      (ASCII Unit Separator). Use `delimiter: "|"` if you want a
      readable form (at the cost of ambiguity if any field value
      contains the chosen delimiter).

  ## Example

      iex> %MyApp.Person{first_name: "Alice", age: 30}
      ...> |> MyApp.Person.canonical_string()
      "Alice<US>30"

  """
  @spec canonical_string(t(), struct() | map(), Keyword.t()) :: String.t()
  def canonical_string(%__MODULE__{} = schema, struct_or_map, opts \\ []) do
    delimiter = Keyword.get(opts, :delimiter, @canonical_delimiter)
    schema |> canonical_field_values(struct_or_map) |> Enum.join(delimiter)
  end

  @doc """
  Returns a binary cryptographic hash of a struct's canonical encoding.

  ## Options

    * `:algorithm` — hash algorithm passed to `:crypto.hash/2`. Default `:sha256`.
    * `:truncate` — bytes to truncate to. Default `16`. `nil` means no truncation.

  See `canonical_string/3` for the encoding rules.
  """
  @spec row_hash(t(), struct() | map(), Keyword.t()) :: binary()
  def row_hash(%__MODULE__{} = schema, struct_or_map, opts \\ []) do
    algorithm = Keyword.get(opts, :algorithm, :sha256)
    truncate = Keyword.get(opts, :truncate, 16)
    canonical = canonical_string(schema, struct_or_map)

    digest = :crypto.hash(algorithm, canonical)

    case truncate do
      nil -> digest
      n when is_integer(n) and n > 0 -> binary_part(digest, 0, min(n, byte_size(digest)))
    end
  end

  # Returns the list of canonical-form field values for a struct/map. Used
  # internally by `canonical_string/3` and `row_hash/3`. Skips derived fields
  # (`:row_hash`, `:raw_row`) and recurses into embeds.
  @doc false
  @spec canonical_field_values(t(), struct() | map()) :: [String.t()]
  def canonical_field_values(%__MODULE__{} = schema, struct_or_map) do
    Enum.flat_map(schema.fields, fn field ->
      canonical_values_for_field(schema, struct_or_map, field)
    end)
  end

  defp canonical_values_for_field(schema, struct_or_map, field) do
    cond do
      Field.derived?(field) -> []
      field.type == :embed -> canonical_embed_values(schema, struct_or_map, field)
      true -> [Field.to_string(Map.get(struct_or_map, field.name), field)]
    end
  end

  defp canonical_embed_values(schema, struct_or_map, field) do
    embed_module = schema.embeds[field.name]
    embed_schema = embed_module.__delimit_schema__()

    case Map.get(struct_or_map, field.name) do
      nil -> List.duplicate("", canonical_field_count(embed_schema))
      embed_value -> canonical_field_values(embed_schema, embed_value)
    end
  end

  # Total number of canonical positions a schema contributes (including
  # nested embeds, excluding derived fields). Used to emit the right
  # number of empty placeholders when an embed is nil.
  @doc false
  @spec canonical_field_count(t()) :: non_neg_integer()
  def canonical_field_count(%__MODULE__{} = schema) do
    Enum.reduce(schema.fields, 0, fn field, acc ->
      cond do
        Field.derived?(field) ->
          acc

        field.type == :embed ->
          embed_module = schema.embeds[field.name]
          embed_schema = embed_module.__delimit_schema__()
          acc + canonical_field_count(embed_schema)

        true ->
          acc + 1
      end
    end)
  end

  @doc """
  Validates that all fields (including flattened embed fields) have a positive integer `width:` option.

  Raises `ArgumentError` if any field is missing `width:` or has a non-positive width.
  """
  @spec validate_fixed_width!(t()) :: :ok
  def validate_fixed_width!(%__MODULE__{} = schema) do
    all_fields = collect_all_fields(schema)

    Enum.each(all_fields, fn {display_name, field} ->
      width = Keyword.get(field.opts, :width)

      cond do
        is_nil(width) ->
          raise ArgumentError,
                "Fixed-width format requires a :width option on all fields, " <>
                  "but field #{inspect(display_name)} is missing :width"

        not is_integer(width) or width <= 0 ->
          raise ArgumentError,
                "Fixed-width format requires a positive integer :width, " <>
                  "but field #{inspect(display_name)} has width: #{inspect(width)}"

        true ->
          :ok
      end
    end)

    :ok
  end

  @doc """
  Returns a flat list of `{display_name, Field.t()}` tuples for all leaf fields,
  including fields from flattened embeds.

  For regular fields, `display_name` is the field name atom.
  For embed fields, `display_name` includes the embed prefix (e.g., `:billing_address_street`).
  """
  @spec collect_all_fields(t()) :: [{atom(), Field.t()}]
  def collect_all_fields(%__MODULE__{} = schema) do
    regular_fields =
      schema.fields
      |> Enum.filter(fn field -> field.type != :embed end)
      |> Enum.map(fn field -> {field.name, field} end)

    embed_fields =
      schema
      |> get_embeds()
      |> Enum.flat_map(fn embed_field ->
        embed_module = schema.embeds[embed_field.name]
        embed_schema = embed_module.__delimit_schema__()
        prefix = get_embed_prefix(embed_field)

        embed_schema.fields
        |> Enum.filter(fn f -> f.type != :embed end)
        |> Enum.map(fn field ->
          display_name = String.to_atom(prefix <> Atom.to_string(field.name))
          {display_name, field}
        end)
      end)

    regular_fields ++ embed_fields
  end

  @doc """
  Returns a list of `{Field.t(), start_offset, width}` tuples using cumulative offsets.

  Used by the fixed-width reader to slice lines into field values.
  """
  @spec field_widths(t()) :: [{Field.t(), non_neg_integer(), pos_integer()}]
  def field_widths(%__MODULE__{} = schema) do
    all_fields = collect_all_fields(schema)

    {result, _offset} =
      Enum.map_reduce(all_fields, 0, fn {_display_name, field}, offset ->
        width = Keyword.fetch!(field.opts, :width)
        {{field, offset, width}, offset + width}
      end)

    result
  end

  @doc """
  Builds a struct (including embeds) from a flat list of raw string values.

  This is needed for fixed-width format where fields are position-based rather than
  header-based. Uses `Field.parse_value/2` for each value.
  """
  @spec to_struct_from_flat_values(t(), [String.t() | nil], Keyword.t()) :: struct()
  def to_struct_from_flat_values(%__MODULE__{} = schema, values, opts \\ []) do
    struct = struct(schema.module)

    # Non-embed, non-derived fields participate in positional column mapping.
    regular_fields =
      Enum.filter(schema.fields, fn field ->
        field.type != :embed and not Field.derived?(field)
      end)

    # Process regular fields positionally
    {struct_with_fields, remaining_values} =
      Enum.reduce(regular_fields, {struct, values}, fn field, {acc, [raw_value | rest]} ->
        raw_value = if raw_value == "", do: nil, else: raw_value

        raw_value =
          if is_nil(raw_value) && Keyword.has_key?(field.opts, :default),
            do: Keyword.get(field.opts, :default),
            else: raw_value

        field_with_opts = %{
          field
          | opts: Keyword.merge(field.opts, Keyword.take(opts, [:trim_fields]))
        }

        parsed_value = Field.parse_value(raw_value, field_with_opts)
        {Map.put(acc, field.name, parsed_value), rest}
      end)

    # Process embed fields
    embed_fields = get_embeds(schema)

    {struct_with_embeds, _remaining} =
      Enum.reduce(embed_fields, {struct_with_fields, remaining_values}, fn embed_field,
                                                                           {acc, vals} ->
        embed_module = schema.embeds[embed_field.name]
        embed_schema = embed_module.__delimit_schema__()

        embed_regular_fields =
          Enum.filter(embed_schema.fields, fn f ->
            f.type != :embed and not Field.derived?(f)
          end)

        field_count = length(embed_regular_fields)

        {embed_values, rest} = Enum.split(vals, field_count)

        # Pad with nils if we ran out of values
        embed_values =
          if length(embed_values) < field_count do
            embed_values ++ List.duplicate(nil, field_count - length(embed_values))
          else
            embed_values
          end

        embed_struct = to_struct_from_flat_values(embed_schema, embed_values, opts)
        {Map.put(acc, embed_field.name, embed_struct), rest}
      end)

    populate_derived(schema, struct_with_embeds, values)
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

      :row_hash ->
        # Derived field — populated post-parse with a binary hash.
        quote do: binary() | nil

      :raw_row ->
        # Derived field — populated post-parse with the raw column list.
        quote do: [String.t()] | nil

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
