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
    # Extract format option if present
    {format, custom_opts} = Keyword.pop(opts, :format)
    
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
    
    # Merge options from schema, format, and function call
    options = Delimit.Formats.merge_options(schema.options, format, custom_opts)

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
      rows = prepare_data_rows(schema, data)
  
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
  defp write_string_optimized(%Schema{} = schema, data, _options, parser) do
    # Process rows in chunks to avoid excessive memory usage
    chunk_size = 1000
    
    # Build iodata in chunks
    chunks = 
      data
      |> Stream.chunk_every(chunk_size)
      |> Stream.map(fn chunk -> 
        rows = prepare_data_rows(schema, chunk)
        parser.dump_to_iodata(rows)
      end)
      |> Enum.to_list()
    
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
    # Extract format option if present
    {format, custom_opts} = Keyword.pop(opts, :format)
    
    # Merge options from schema, format, and function call
    options = Delimit.Formats.merge_options(schema.options, format, custom_opts)

    # Get the parser with our options
    parser = get_parser(options)

    # Open file for writing
    {:ok, file} = File.open(path, [:write, :utf8, :delayed_write])

    # Process in chunks to optimize memory usage and improve performance
    chunk_size = 1000
    
    # Stream and write in chunks
    _result =
      data_stream
      |> Stream.chunk_every(chunk_size)
      |> Stream.each(fn chunk ->
        rows = prepare_data_rows(schema, chunk)
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
      
    # For writing, we never want to skip headers since we handle that separately
    skip_headers = false
      
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
        Delimit.Parsers.get_parser_with_escape(delimiter, escape, skip_headers: skip_headers)
      end
    else
      # Use the optimized parsers module with default escape
      parser = Delimit.Parsers.get_parser(delimiter, skip_headers: skip_headers)
        
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
  defp prepare_data_rows(schema, data) do
    Enum.map(data, fn item ->
      Schema.to_row(schema, item)
    end)
  end
end
