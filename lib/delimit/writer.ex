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
  * `:escape` - The escape character used for quotes (default: double-quote)
  * `:line_ending` - Line ending to use (default: system-dependent)
  """
  @type write_options :: [
          headers: boolean(),
          delimiter: String.t(),
          escape: String.t(),
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
    # Fast path for small datasets (batch processing)
    if length(data) < 1000 do
      # Standard process for small datasets
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
    else
      # Optimized path for large datasets
      write_string_optimized(schema, data, options, parser) 
    end
  end
  
  # Optimized version that processes data in chunks and uses iodata more efficiently
  defp write_string_optimized(%Schema{} = schema, data, options, parser) do
    # Write headers if needed
    header_iodata = 
      if Keyword.get(options, :headers, true) do
        headers = collect_headers(schema)
        parser.dump_to_iodata([headers])
      else
        []
      end
      
    # Process rows in chunks to avoid excessive memory usage
    chunk_size = 1000
    
    # Build iodata in chunks
    chunks = 
      data
      |> Stream.chunk_every(chunk_size)
      |> Stream.map(fn chunk -> 
        rows = prepare_data_rows(schema, chunk, nil)
        parser.dump_to_iodata(rows)
      end)
      |> Enum.to_list()
    
    # Combine header and data chunks
    [header_iodata | chunks]
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
    {:ok, file} = File.open(path, [:write, :utf8, :delayed_write])

    # Write headers if needed
    _header_positions = 
      if Keyword.get(options, :headers, true) do
        headers = collect_headers(schema)
        header_row = parser.dump_to_iodata([headers])
        IO.binwrite(file, header_row)
        
        # Cache header positions for efficient row conversion
        cache_header_positions(schema, headers)
      else
        %{}
      end

    # Process in chunks to optimize memory usage and improve performance
    chunk_size = 1000
    
    # Stream and write in chunks
    _result =
      data_stream
      |> Stream.chunk_every(chunk_size)
      |> Stream.each(fn chunk ->
        rows = prepare_data_rows(schema, chunk, nil)
        rows_iodata = parser.dump_to_iodata(rows)
        IO.binwrite(file, rows_iodata)
      end)
      |> Stream.run()

    # Close the file
    File.close(file)

    :ok
  end

  # Get a CSV parser with the given options
  defp get_parser(options) do
    delimiter = Keyword.get(options, :delimiter, ",")
    line_ending = Keyword.get(options, :line_ending, "\n")
    escape = Keyword.get(options, :escape)
    
    if escape do
      # Custom escape character specified
      if line_ending != "\n" do
        # Both custom escape and line ending
        unique_module_name =
          String.to_atom("DelimitCustomParser_#{System.unique_integer([:positive])}")
        
        NimbleCSV.define(unique_module_name, separator: delimiter, escape: escape, line_separator: line_ending)
        unique_module_name
      else
        # Only custom escape
        Delimit.Parsers.get_parser_with_escape(delimiter, escape)
      end
    else
      # Use the optimized parsers module with default escape
      parser = Delimit.Parsers.get_parser(delimiter)
      
      # For custom line endings, re-define the parser
      if line_ending != "\n" do
        unique_module_name =
          String.to_atom("DelimitLineEndingParser_#{System.unique_integer([:positive])}")
        
        NimbleCSV.define(unique_module_name, separator: delimiter, line_separator: line_ending)
        unique_module_name
      else
        parser
      end
    end
  end

  # Prepare data rows for writing
  defp prepare_data_rows(schema, data, headers) do
    # Fast-path for performance when headers are known
    # Cache header positions for optimized row conversion
    if headers do
      header_positions = cache_header_positions(schema, headers)
      
      Enum.map(data, fn item ->
        Schema.to_row(schema, item, headers, header_positions)
      end)
    else
      Enum.map(data, fn item ->
        Schema.to_row(schema, item, headers)
      end)
    end
  end
  
  # Cache header positions for faster field lookups
  defp cache_header_positions(schema, headers) do
    schema.fields
    |> Enum.filter(fn field -> field.type != :embed end)
    |> Enum.map(fn field -> 
      label = field.opts[:label] || Atom.to_string(field.name)
      {field.name, Enum.find_index(headers, &(&1 == label))}
    end)
    |> Map.new()
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
