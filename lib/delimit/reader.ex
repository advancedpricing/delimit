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
  * `:escape` - The escape character used for quotes (default: double-quote)
  * `:skip_lines` - Number of lines to skip at the beginning (default: 0)
  * `:skip_while` - Function that returns true for lines to skip
  * `:trim_fields` - Whether to trim whitespace from fields (default: true)
  * `:nil_on_empty` - Convert empty strings to nil (default: true)
  * `:format` - Predefined format (`:csv`, `:tsv`, `:psv`) that sets appropriate options
  """
  @type read_options :: [
          headers: boolean(),
          delimiter: String.t(),
          escape: String.t(),
          skip_lines: non_neg_integer(),
          skip_while: (String.t() -> boolean()),
          trim_fields: boolean(),
          nil_on_empty: boolean(),
          format: atom()
        ]

  @doc """
  Reads delimited data from a file.

  ## Parameters

    * `schema` - The schema definition
    * `path` - Path to the delimited file
    * `opts` - Read options that override schema options

  ## Returns

    * List of structs with parsed data based on schema

  ## Examples

      iex> MyApp.Person.read("people.csv")
      [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}, ...]

      iex> MyApp.Person.read("people.tsv", format: :tsv)
      [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}, ...]
  """
  @spec read_file(Schema.t(), Path.t(), read_options()) :: [struct()]
  def read_file(%Schema{} = schema, path, opts \\ []) do
    # Extract format option if present
    {format, custom_opts} = Keyword.pop(opts, :format)
    
    # Merge options from schema, format, and function call
    options = Delimit.Formats.merge_options(schema.options, format, custom_opts)

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

  ## Examples

      iex> csv_data = "first_name,last_name,age\\nJohn,Doe,42"
      iex> MyApp.Person.read_string(csv_data)
      [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}]

      iex> tsv_data = "first_name\\tlast_name\\tage\\nJohn\\tDoe\\t42"
      iex> MyApp.Person.read_string(tsv_data, format: :tsv)
      [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}]
  """
  @spec read_string(Schema.t(), binary(), read_options()) :: [struct()]
  def read_string(%Schema{} = schema, string, opts \\ []) when is_binary(string) do
    # Extract format option if present
    {format, custom_opts} = Keyword.pop(opts, :format)
    
    # Merge options from schema, format, and function call
    options = Delimit.Formats.merge_options(schema.options, format, custom_opts)

    # Get parser
    parser = get_parser(options)

    # Call internal implementation
    read_string(schema, string, options, parser)
  end

  # Internal implementation with parser provided
  @spec read_string(Schema.t(), binary(), read_options(), module()) :: [struct()]
  defp read_string(%Schema{} = schema, string, options, parser) when is_binary(string) do
    # Check if we should extract headers explicitly
    has_headers = Keyword.get(options, :headers, true)
    
    # Get header line if headers enabled
    {headers, data_string} = if has_headers do
      case String.split(string, "\n", parts: 2) do
        [header_line, rest] ->
          # For header parsing, we'll manually split because NimbleCSV parsers
          # seem to handle the header row differently
          headers = String.split(header_line, Keyword.get(options, :delimiter, ","))
                   |> Enum.map(&String.trim/1)
          {headers, rest}
        _ ->
          {[], string}
      end
    else
      {nil, string}
    end
    
    # Parse the data rows
    data_rows = if String.trim(data_string) != "", do: parse_with_options(data_string, options, parser), else: []
    
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
    # Cache header positions once (only if headers list is non-empty)
    header_positions = if headers != [], do: cache_header_positions(schema, headers), else: nil
    
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

  ## Examples

      iex> MyApp.Person.stream("large_people_file.csv")
      iex> |> Stream.take(10)
      iex> |> Enum.to_list()
      [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}, ...]

      iex> MyApp.Person.stream("large_people_file.tsv", format: :tsv)
      iex> |> Stream.take(10)
      iex> |> Enum.to_list()
      [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}, ...]
  """
  @spec stream_file(Schema.t(), Path.t(), read_options()) :: Enumerable.t()
  def stream_file(%Schema{} = schema, path, opts \\ []) do
    # Extract format option if present
    {format, custom_opts} = Keyword.pop(opts, :format)
    
    # Merge options from schema, format, and function call
    options = Delimit.Formats.merge_options(schema.options, format, custom_opts)

    # Get parser
    parser = get_parser(options)

    # Check if headers are enabled
    has_headers = Keyword.get(options, :headers, true)

    # Handle headers specially for streaming
    if has_headers do
      # Open file for reading header separately
      case File.read(path) do
        {:ok, content} ->
          # Split the file content to get header line
          case String.split(content, "\n", parts: 2) do
            [header_line, _rest] ->
              # For header parsing, we'll manually split because NimbleCSV parsers
              # seem to handle the header row differently
              headers = String.split(header_line, Keyword.get(options, :delimiter, ","))
                       |> Enum.map(&String.trim/1)

              # Now set up the stream for data rows, skipping the header line
              stream =
                path
                |> File.stream!()
                |> Stream.drop(1)
                |> stream_with_options(options, parser)

              # Cache header positions for better performance (only if headers list is non-empty)
              header_positions = if headers != [], do: cache_header_positions(schema, headers), else: nil

              # Map each row to a struct with optimized header lookup
              Stream.map(stream, fn row ->
                Schema.to_struct(schema, row, headers, header_positions)
              end)
            _ ->
              # Empty file or only header line
              Stream.map([], fn _ -> nil end)
          end
        _ ->
          # File couldn't be read, return empty stream
          Stream.map([], fn _ -> nil end)
      end
    else
      # No headers, just map each row to a struct
      stream =
        path
        |> File.stream!()
        |> stream_with_options(options, parser)

      Stream.map(stream, fn row ->
        Schema.to_struct(schema, row, nil)
      end)
    end
  end

  # Get a parser with the given options
  defp get_parser(options) do
    delimiter = Keyword.get(options, :delimiter, ",")
    escape = Keyword.get(options, :escape)
    
    if escape do
      # Use parser with custom escape character
      Delimit.Parsers.get_parser_with_escape(delimiter, escape)
    else
      # Use default parser (double quote as escape)
      Delimit.Parsers.get_parser(delimiter)
    end
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
    
    # Make sure there's content to parse  
    if String.trim(string) == "" do
      []
    else
      # Parse the string using NimbleCSV - headers are handled separately at the calling level
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
end