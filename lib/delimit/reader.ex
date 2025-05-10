defmodule Delimit.Reader do
  @moduledoc """
  Functions for reading delimited data from files or strings.

  This module provides functionality to read delimited data from files
  or strings based on schema definitions, supporting both single-read
  and streaming operations.
  """

  alias Delimit.Schema

  @typedoc """
  Options for reading delimited data.

  * `:headers` - Whether the first row contains headers (default: true)
  * `:delimiter` - The field delimiter character (default: comma)
  * `:skip_lines` - Number of lines to skip at the beginning (default: 0)
  * `:skip_while` - Function that returns true for lines to skip
  * `:trim_fields` - Whether to trim whitespace from fields (default: true)
  * `:nil_on_empty` - Convert empty strings to nil (default: true)
  """
  @type read_options :: [
          headers: boolean(),
          delimiter: String.t(),
          skip_lines: non_neg_integer(),
          skip_while: (String.t() -> boolean()),
          trim_fields: boolean(),
          nil_on_empty: boolean()
        ]

  @doc """
  Reads delimited data from a file.

  ## Parameters

    * `schema` - The schema definition
    * `path` - Path to the delimited file
    * `opts` - Read options that override schema options

  ## Returns

    * List of structs with parsed data based on schema

  ## Example

      iex> MyApp.Person.read("people.csv")
      [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}, ...]
  """
  @spec read_file(Schema.t(), Path.t(), read_options()) :: [struct()]
  def read_file(%Schema{} = schema, path, opts \\ []) do
    # Merge options from schema and function call
    options = Keyword.merge(schema.options, opts)

    # Get the parser 
    parser = get_parser(options)

    # Read the file
    contents = File.read!(path)
    read_string(schema, contents, options, parser)
  end

  @doc """
  Reads delimited data from a string.

  ## Parameters

    * `schema` - The schema definition
    * `string` - String containing delimited data
    * `opts` - Read options that override schema options

  ## Returns

    * List of structs with parsed data based on schema

  ## Example

      iex> csv_data = "first_name,last_name,age\\nJohn,Doe,42"
      iex> MyApp.Person.read_string(csv_data)
      [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}]
  """
  @spec read_string(Schema.t(), binary(), read_options()) :: [struct()]
  def read_string(%Schema{} = schema, string, opts \\ []) when is_binary(string) do
    # Merge options from schema and function call
    options = Keyword.merge(schema.options, opts)

    # Get parser
    parser = get_parser(options)

    # Call internal implementation
    read_string(schema, string, options, parser)
  end

  # Internal implementation with parser provided
  @spec read_string(Schema.t(), binary(), read_options(), module()) :: [struct()]
  defp read_string(%Schema{} = schema, string, options, parser) when is_binary(string) do
    # Parse the string
    rows = parse_with_options(string, options, parser)

    # Extract headers if needed
    {headers, data_rows} = extract_headers(rows, options)

    # For large data sets, use batched processing
    if length(data_rows) > 1000 do
      read_string_batched(schema, data_rows, headers)
    else
      # Standard processing for smaller datasets
      Enum.map(data_rows, fn row ->
        Schema.to_struct(schema, row, headers)
      end)
    end
  end
  
  # Process larger datasets in batches
  defp read_string_batched(%Schema{} = schema, data_rows, headers) do
    # Cache header positions once
    header_positions = if headers, do: cache_header_positions(schema, headers), else: nil
    
    # Process in batches of 1000
    data_rows
    |> Stream.chunk_every(1000)
    |> Enum.flat_map(fn batch ->
      Enum.map(batch, fn row ->
        Schema.to_struct(schema, row, headers, header_positions)
      end)
    end)
  end
  
  # Cache header positions for optimized field lookups
  defp cache_header_positions(%Schema{} = schema, headers) do
    schema.fields
    |> Enum.filter(fn field -> field.type != :embed end)
    |> Enum.map(fn field ->
      header_name = field.opts[:label] || Atom.to_string(field.name)
      {field.name, Enum.find_index(headers, fn h -> h == header_name end)}
    end)
    |> Map.new()
  end

  @doc """
  Streams delimited data from a file.

  ## Parameters

    * `schema` - The schema definition
    * `path` - Path to the delimited file
    * `opts` - Read options that override schema options

  ## Returns

    * Stream of structs with parsed data based on schema

  ## Example

      iex> MyApp.Person.stream("large_people_file.csv")
      iex> |> Stream.take(10)
      iex> |> Enum.to_list()
      [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}, ...]
  """
  @spec stream_file(Schema.t(), Path.t(), read_options()) :: Enumerable.t()
  def stream_file(%Schema{} = schema, path, opts \\ []) do
    # Merge options from schema and function call
    options = Keyword.merge(schema.options, opts)

    # Get parser
    parser = get_parser(options)

    # Create the base stream
    stream =
      path
      |> File.stream!()
      |> stream_with_options(options, parser)

    # If headers are enabled, we need to handle them
    if Keyword.get(options, :headers, true) do
      {headers_stream, data_stream} = extract_headers_from_stream(stream)
      
      # Cache header positions for better performance
      header_positions = if headers_stream, do: cache_header_positions(schema, headers_stream), else: nil

      # Map each row to a struct with optimized header lookup
      Stream.map(data_stream, fn row ->
        Schema.to_struct(schema, row, headers_stream, header_positions)
      end)
    else
      # No headers, just map each row to a struct
      Stream.map(stream, fn row ->
        Schema.to_struct(schema, row, nil)
      end)
    end
  end

  # Get a parser with the given options
  defp get_parser(options) do
    delimiter = Keyword.get(options, :delimiter, ",")
    Delimit.Parsers.get_parser(delimiter)
  end

  # Parse a string with the given options
  defp parse_with_options(string, options, parser) do
    # Apply CSV parsing - normalize line endings first for consistency
    string = 
      if String.contains?(string, "\r\n") do
        String.replace(string, "\r\n", "\n")
      else
        string
      end
      
    # Parse the string using NimbleCSV
    rows = parser.parse_string(string)

    # Apply skipping options
    skip_lines = Keyword.get(options, :skip_lines, 0)
    skip_fn = Keyword.get(options, :skip_while)

    rows =
      if skip_lines > 0 do
        Enum.drop(rows, skip_lines)
      else
        rows
      end

    # Apply skip_while if provided
    if skip_fn do
      delimiter = Keyword.get(options, :delimiter, ",")
      Enum.drop_while(rows, fn row ->
        raw_line = Enum.join(row, delimiter)
        skip_fn.(raw_line)
      end)
    else
      rows
    end
  end

  # Stream a file with the given options
  defp stream_with_options(stream, options, parser) do
    # Apply CSV parsing
    stream = parser.parse_stream(stream)

    # Apply skipping options
    skip_lines = Keyword.get(options, :skip_lines, 0)
    skip_fn = Keyword.get(options, :skip_while)

    stream =
      if skip_lines > 0 do
        Stream.drop(stream, skip_lines)
      else
        stream
      end

    # Apply skip_while if provided
    if skip_fn do
      Stream.drop_while(stream, fn row ->
        raw_line = Enum.join(row, Keyword.get(options, :delimiter, ","))
        skip_fn.(raw_line)
      end)
    else
      stream
    end
  end

  # Extract headers from rows
  defp extract_headers(rows, options) do
    if Keyword.get(options, :headers, true) and length(rows) > 0 do
      [headers | data_rows] = rows
      {headers, data_rows}
    else
      {nil, rows}
    end
  end

  # Extract headers from a stream
  defp extract_headers_from_stream(stream) do
    # Use Stream.resource to efficiently handle the stream
    {first_row, rest_stream} = 
      case Enum.take(stream, 1) do
        [headers] -> {headers, Stream.drop(stream, 1)}
        [] -> {nil, stream}
        _ -> {nil, stream}
      end
    
    {first_row, rest_stream}
  end
end
