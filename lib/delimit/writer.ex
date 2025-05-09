defmodule Delimit.Writer do
  @moduledoc """
  Functions for writing delimited data to files or strings.

  This module provides functionality to write delimited data to files
  or strings based on schema definitions, supporting both single-write
  and streaming operations.
  """

  alias Delimit.Schema

  @typedoc """
  Options for writing delimited data.

  * `:headers` - Whether to include headers in the output (default: true)
  * `:delimiter` - The field delimiter character (default: comma)
  * `:line_ending` - Line ending to use (default: system-dependent)
  """
  @type write_options :: [
          headers: boolean(),
          delimiter: String.t(),
          line_ending: String.t()
        ]

  @doc """
  Writes delimited data to a file.

  ## Parameters

    * `schema` - The schema definition
    * `path` - Path to the output file
    * `data` - List of structs to write
    * `opts` - Write options that override schema options

  ## Returns

    * `:ok` on success

  ## Example

      iex> people = [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}]
      iex> MyApp.Person.write("people.csv", people)
      :ok
  """
  @spec write_file(Schema.t(), Path.t(), [struct()], write_options()) :: :ok
  def write_file(%Schema{} = schema, path, data, opts \\ []) do
    # Merge options from schema and function call
    options = Keyword.merge(schema.options, opts)

    # Convert data to string using the appropriate parser
    parser = get_parser(options)
    csv_string = write_string(schema, data, options, parser)

    # Write to file
    File.write!(path, csv_string)
  end

  @doc """
  Writes delimited data to a string.

  ## Parameters

    * `schema` - The schema definition
    * `data` - List of structs to write
    * `opts` - Write options that override schema options

  ## Returns

    * String containing the delimited data

  ## Example

      iex> people = [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}]
      iex> MyApp.Person.write_string(people)
      "first_name,last_name,age\\nJohn,Doe,42\\n"
  """
  @spec write_string(Schema.t(), [struct()], write_options()) :: binary()
  def write_string(%Schema{} = schema, data, opts \\ []) when is_list(data) do
    # Merge options from schema and function call
    options = Keyword.merge(schema.options, opts)

    # Get the parser with our options
    parser = get_parser(options)

    # Call the internal implementation with the parser
    write_string(schema, data, options, parser)
  end

  # Internal implementation with parser provided
  @spec write_string(Schema.t(), [struct()], write_options(), module()) :: binary()
  defp write_string(%Schema{} = schema, data, options, parser) do
    # Prepare headers if needed
    rows =
      if Keyword.get(options, :headers, true) do
        # Get all headers including those from embedded schemas
        headers = collect_headers(schema)
        [headers | prepare_data_rows(schema, data, headers)]
      else
        prepare_data_rows(schema, data, nil)
      end

    # Generate the delimited string
    rows
    |> parser.dump_to_iodata()
    |> IO.iodata_to_binary()
  end

  @doc """
  Streams delimited data to a file.

  ## Parameters

    * `schema` - The schema definition
    * `path` - Path to the output file
    * `data_stream` - Stream of structs to write
    * `opts` - Write options that override schema options

  ## Returns

    * `:ok` on success

  ## Example

      iex> stream = Stream.map(1..1000, fn i -> %MyApp.Person{first_name: "User", last_name: "User", age: i} end)
      iex> MyApp.Person.stream_to_file("people.csv", stream)
      :ok
  """
  @spec stream_to_file(Schema.t(), Path.t(), Enumerable.t(), write_options()) :: :ok
  def stream_to_file(%Schema{} = schema, path, data_stream, opts \\ []) do
    # Merge options from schema and function call
    options = Keyword.merge(schema.options, opts)

    # Get the parser with our options
    parser = get_parser(options)

    # Open file for writing
    {:ok, file} = File.open(path, [:write, :utf8])

    # Write headers if needed
    if Keyword.get(options, :headers, true) do
      headers = collect_headers(schema)
      header_row = parser.dump_to_iodata([headers])
      IO.binwrite(file, header_row)
    end

    # Stream each item, convert to row, and write to file
    _result =
      data_stream
      |> Stream.map(fn item ->
        row = Schema.to_row(schema, item)
        parser.dump_to_iodata([row])
      end)
      |> Enum.each(fn row_data ->
        IO.binwrite(file, row_data)
      end)

    # Close the file
    File.close(file)

    :ok
  end

  # Get a CSV parser with the given options
  defp get_parser(options) do
    delimiter = Keyword.get(options, :delimiter, ",")
    line_ending = Keyword.get(options, :line_ending, "\n")

    # Create a unique module name to avoid redefinition warnings
    unique_module_name =
      String.to_atom("DelimitDynamicParser_#{System.unique_integer([:positive])}")

    # Create a dynamic parser with our options
    result =
      NimbleCSV.define(unique_module_name, separator: delimiter, line_separator: line_ending)

    # Extract the module name from the result
    case result do
      # When it returns the module directly
      module when is_atom(module) -> module
      # When it returns a tuple with module info
      {:module, module, _binary, _term} -> module
      # Fall back to the name if something else is returned
      _ -> unique_module_name
    end
  end

  # Prepare data rows for writing
  defp prepare_data_rows(schema, data, headers) do
    Enum.map(data, fn item ->
      Schema.to_row(schema, item, headers)
    end)
  end

  # Collect all headers from schema, including those from embedded schemas
  # exposed for testing
  @doc false
  def collect_headers(schema) do
    # Get regular field headers
    regular_headers = Schema.headers(schema)

    # Get headers from embedded schemas
    embed_headers =
      schema
      |> Schema.get_embeds()
      |> Enum.flat_map(fn field ->
        embed_module = schema.embeds[field.name]
        embed_schema = embed_module.__delimit_schema__()
        prefix = Schema.get_embed_prefix(field)

        # Get headers with the prefix applied
        Schema.headers(embed_schema, prefix)
      end)

    # Combine regular and embedded headers
    regular_headers ++ embed_headers
  end
end
