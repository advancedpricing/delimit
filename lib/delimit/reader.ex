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

  * `:delimiter` - The field delimiter character (default: comma)
  * `:escape` - The escape character used for quotes (default: double-quote)
  * `:skip_lines` - Number of lines to skip at the beginning (default: 0)
  * `:skip_while` - Function that returns true for lines to skip
  * `:trim_fields` - Whether to trim whitespace from fields (default: true)
  * `:nil_on_empty` - Convert empty strings to nil (default: true)
  * `:format` - Predefined format (`:csv`, `:tsv`, `:psv`) that sets appropriate options
  """
  @type read_options :: [
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

    # Read the file
    case File.read(path) do
      {:ok, content} ->
        read_string(schema, content, options)

      {:error, reason} ->
        raise "Failed to read file: #{reason}"
    end
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

    # Skip empty input
    if String.trim(string) == "" do
      []
    else
      # Get key options
      delimiter = Keyword.get(options, :delimiter, ",")
      escape = Keyword.get(options, :escape, "\"")
      skip_lines = Keyword.get(options, :skip_lines, 0)
      skip_while_fn = Keyword.get(options, :skip_while)

      # Parse directly with NimbleCSV, which handles CSV properly
      parser = Delimit.Parsers.get_parser_with_escape(delimiter, escape, skip_headers: false)

      # Split into lines and apply preprocessing
      lines = String.split(string, "\r\n")
      filtered_lines = preprocess_lines(lines, skip_lines, skip_while_fn)
      # Ensure string ends with CRLF for proper NimbleCSV parsing
      adjusted_string = Enum.join(filtered_lines, "\r\n") <> "\r\n"

      # Skip empty input after preprocessing
      if String.trim(adjusted_string) == "" do
        []
      else
        # Parse all rows
        all_rows =
          try do
            parser.parse_string(adjusted_string)
          rescue
            _ ->
              IO.puts(
                "Warning: Initial parsing failed, trying with more lenient configuration"
              )

              lenient_parser =
                Delimit.Parsers.get_parser_with_escape(delimiter, escape)

              lenient_parser.parse_string(adjusted_string)
          end

        # All rows are data - convert all rows to structs
        # Process all rows as data
        all_rows
        |> Enum.reject(fn row -> length(row) == 0 || Enum.all?(row, &(&1 == "")) end)
        |> Enum.map(fn row ->
          Schema.to_struct(schema, row)
        end)
      end
    end
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

    # Check if file exists
    if !File.exists?(path) do
      raise "File does not exist: #{path}"
    end

    # Get key options
    delimiter = Keyword.get(options, :delimiter, ",")
    escape = Keyword.get(options, :escape, "\"")
    skip_lines = Keyword.get(options, :skip_lines, 0)
    skip_while_fn = Keyword.get(options, :skip_while)

    # Simple case - use NimbleCSV directly with no preprocessing
    if skip_lines == 0 && skip_while_fn == nil do
      # Make sure to use a parser that doesn't skip any rows
      parser = Delimit.Parsers.get_parser_with_escape(delimiter, escape)

      path
      |> File.stream!()
      |> parser.parse_stream()
      |> Stream.reject(fn row -> length(row) == 0 || Enum.all?(row, &(&1 == "")) end)
      |> Stream.map(fn row ->
        Schema.to_struct(schema, row)
      end)
    else
      # For skip options, we need preprocessing
      stream_with_preprocessing(schema, path, options)
    end
  end

  # Process lines: handle skips, empty lines, comments
  defp preprocess_lines(lines, skip_lines, skip_while_fn) do
    # Apply skip_while function if provided
    lines = if skip_while_fn, do: Enum.drop_while(lines, skip_while_fn), else: lines

    # Apply skip_lines option
    lines = if skip_lines > 0, do: Enum.drop(lines, skip_lines), else: lines

    lines
  end

  # Handle streaming with preprocessing for skipping lines
  defp stream_with_preprocessing(schema, path, options) do
    # Get key options
    delimiter = Keyword.get(options, :delimiter, ",")
    escape = Keyword.get(options, :escape, "\"")
    skip_lines = Keyword.get(options, :skip_lines, 0)
    skip_while_fn = Keyword.get(options, :skip_while)

    # Create parser - no skip_headers
    parser = Delimit.Parsers.get_parser_with_escape(delimiter, escape)

    # First, read and process the file
    {:ok, content} = File.read(path)

    # Split into lines and apply preprocessing (using CRLF for better NimbleCSV compatibility)
    lines = String.split(content, "\r\n")
    filtered_lines = preprocess_lines(lines, skip_lines, skip_while_fn)

    # Ensure string ends with CRLF for proper NimbleCSV parsing
    adjusted_content = Enum.join(filtered_lines, "\r\n") <> "\r\n"

    # Parse all rows
    all_rows = parser.parse_string(adjusted_content)

    if all_rows == [] do
      # No valid rows after parsing
      Stream.map([], & &1)
    else
      # Create a stream directly from the rows - ensure we get ALL rows
      data_stream =
        Stream.map(all_rows, fn row ->
          # Skip empty rows
          if Enum.all?(row, &(&1 == "")) do
            nil
          else
            Schema.to_struct(schema, row)
          end
        end)

      # Filter out nil values (from empty rows)
      Stream.reject(data_stream, &is_nil/1)
    end
  end

  # This function was removed as it's no longer needed
  # We now use preprocess_lines instead

  # This function was removed as we no longer use header-based mapping
end
