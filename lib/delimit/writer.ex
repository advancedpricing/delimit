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

  * `:delimiter` - The field delimiter character (default: comma)
  * `:escape` - The escape character used for quotes (default: double-quote)
  * `:line_ending` - Line ending to use (default: system-dependent)
  * `:format` - Predefined format (`:csv`, `:tsv`, `:psv`) that sets appropriate options
  """
  @type write_options :: [
          delimiter: String.t(),
          escape: String.t(),
          line_ending: String.t(),
          format: atom()
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
    # Extract format option if present or infer from file extension
    format =
      case Keyword.get(opts, :format) do
        nil ->
          # Try to infer format from file extension
          ext = path |> Path.extname() |> String.downcase()

          case ext do
            ".csv" -> :csv
            ".tsv" -> :tsv
            ".psv" -> :psv
            ".ssv" -> :ssv
            _ -> nil
          end

        format ->
          format
      end

    {_, custom_opts} = Keyword.pop(opts, :format)

    # Merge options from schema, format, and function call
    options = Delimit.Formats.merge_options(schema.options, format, custom_opts)

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
      
      iex> people = [%MyApp.Person{first_name: "John", last_name: "Doe", age: 42}]
      iex> MyApp.Person.write_string(people, format: :tsv)
      "first_name\\tlast_name\\tage\\nJohn\\tDoe\\t42\\n"
  """
  @spec write_string(Schema.t(), [struct()], write_options()) :: binary()
  def write_string(%Schema{} = schema, data, opts \\ []) when is_list(data) do
    # Extract format option if present
    {format, custom_opts} = Keyword.pop(opts, :format)

    # Handle headers option
    {headers_opt, remaining_opts} = Keyword.pop(custom_opts, :headers)

    # Merge options from schema, format, and function call
    merged_opts =
      if is_nil(headers_opt),
        do: remaining_opts,
        else: [{:headers, headers_opt} | remaining_opts]

    options = Delimit.Formats.merge_options(schema.options, format, merged_opts)

    # Get the parser with our options
    parser = get_parser(options)

    # Call the internal implementation with the parser
    write_string(schema, data, options, parser)
  end

  # Internal implementation with parser provided
  @spec write_string(Schema.t(), [struct()], write_options(), module()) :: binary()
  defp write_string(%Schema{} = schema, data, options, parser) do
    # Fast path for small datasets (batch processing)
    if length(data) < 1000 do
      # Standard process for small datasets
      headers_enabled = Keyword.get(options, :headers, false)

      rows =
        if headers_enabled do
          # Add the headers as the first row
          schema_headers = Schema.headers(schema)
          [schema_headers | Enum.map(data, fn item -> Schema.to_row(schema, item) end)]
        else
          # No headers, just process data rows
          Enum.map(data, fn item -> Schema.to_row(schema, item) end)
        end

      # Generate the delimited string
      rows
      |> parser.dump_to_iodata()
      |> IO.iodata_to_binary()
    else
      # Optimized path for large datasets
      write_string_optimized(schema, data, options, parser)
    end
  end

  # Optimized version that processes data in chunks and uses iodata more efficiently
  defp write_string_optimized(%Schema{} = schema, data, options, parser) do
    # Process rows in chunks to avoid excessive memory usage
    chunk_size = 1000

    # Check if headers are enabled
    headers_enabled = Keyword.get(options, :headers, false)

    # Build iodata in chunks
    chunks =
      if headers_enabled do
        # Add headers as the first chunk
        schema_headers = Schema.headers(schema)
        headers_chunk = parser.dump_to_iodata([schema_headers])

        # Process data chunks
        data_chunks =
          data
          |> Stream.chunk_every(chunk_size)
          |> Stream.map(fn chunk ->
            rows = Enum.map(chunk, fn item -> Schema.to_row(schema, item) end)
            parser.dump_to_iodata(rows)
          end)
          |> Enum.to_list()

        # Combine headers with data chunks
        [headers_chunk | data_chunks]
      else
        # No headers, just process data chunks
        data
        |> Stream.chunk_every(chunk_size)
        |> Stream.map(fn chunk ->
          rows = Enum.map(chunk, fn item -> Schema.to_row(schema, item) end)
          parser.dump_to_iodata(rows)
        end)
        |> Enum.to_list()
      end

    # Combine chunks
    IO.iodata_to_binary(chunks)
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
      
      iex> stream = Stream.map(1..1000, fn i -> %MyApp.Person{first_name: "User", last_name: "User", age: i} end)
      iex> MyApp.Person.stream_to_file("people.tsv", stream, format: :tsv)
      :ok
  """
  @spec stream_to_file(Schema.t(), Path.t(), Enumerable.t(), write_options()) :: :ok
  def stream_to_file(%Schema{} = schema, path, data_stream, opts \\ []) do
    # Extract format option if present or infer from file extension
    format =
      case Keyword.get(opts, :format) do
        nil ->
          # Try to infer format from file extension
          path_string = if is_binary(path), do: path, else: to_string(path)
          ext = path_string |> Path.extname() |> String.downcase()

          case ext do
            ".csv" -> :csv
            ".tsv" -> :tsv
            ".psv" -> :psv
            ".ssv" -> :ssv
            _ -> nil
          end

        format ->
          format
      end

    {_, custom_opts} = Keyword.pop(opts, :format)

    # Extract headers option
    {headers_opt, remaining_opts} = Keyword.pop(custom_opts, :headers)

    # Merge options from schema, format, and function call
    merged_opts =
      if is_nil(headers_opt),
        do: remaining_opts,
        else: [{:headers, headers_opt} | remaining_opts]

    options = Delimit.Formats.merge_options(schema.options, format, merged_opts)

    # Get the parser with our options
    parser = get_parser(options)

    # Create a temporary file first to avoid any issues with streaming
    path_string = if is_binary(path), do: path, else: to_string(path)
    temp_path = path_string <> ".tmp"

    # Ensure temp file is cleaned up
    on_exit = fn ->
      File.rm(temp_path)
    end

    try do
      # Open temp file for writing
      {:ok, file} = File.open(temp_path, [:write, :utf8])

      # Check if headers are enabled
      headers_enabled = Keyword.get(options, :headers, false)

      # Write headers if enabled
      if headers_enabled do
        schema_headers = Schema.headers(schema)
        headers_iodata = parser.dump_to_iodata([schema_headers])
        IO.binwrite(file, headers_iodata)
      end

      # Process the stream in chunks to avoid loading everything into memory
      data_stream
      |> Enum.chunk_every(100)
      |> Enum.each(fn chunk ->
        # Convert items to rows
        rows = Enum.map(chunk, fn item -> Schema.to_row(schema, item) end)

        # Write to file
        IO.binwrite(file, parser.dump_to_iodata(rows))
      end)

      # Close the file
      File.close(file)

      # Move temp file to final destination
      File.rename(temp_path, path_string)

      :ok
    after
      # Cleanup temp file if it exists
      on_exit.()
    end
  end

  # Get a CSV parser with the given options
  defp get_parser(options) do
    delimiter = Keyword.get(options, :delimiter, ",")
    line_ending = Keyword.get(options, :line_ending, "\n")
    escape = Keyword.get(options, :escape)

    # For writing, we never want to skip headers since we handle that separately
    skip_headers = false

    if escape do
      # Custom escape character specified
      if line_ending == "\n" do
        # Only custom escape
        Delimit.Parsers.get_parser_with_escape(delimiter, escape, skip_headers: skip_headers)
        # Both custom escape and line ending
      else
        unique_module_name =
          String.to_atom("DelimitCustomParser_#{System.unique_integer([:positive])}")

        NimbleCSV.define(unique_module_name,
          separator: delimiter,
          escape: escape,
          line_separator: line_ending
        )

        unique_module_name
      end
    else
      # Use the optimized parsers module with default escape
      parser = Delimit.Parsers.get_parser(delimiter, skip_headers: skip_headers)

      # For custom line endings, re-define the parser
      if line_ending == "\n" do
        parser
      else
        unique_module_name =
          String.to_atom("DelimitLineEndingParser_#{System.unique_integer([:positive])}")

        NimbleCSV.define(unique_module_name, separator: delimiter, line_separator: line_ending)
        unique_module_name
      end
    end
  end
end
