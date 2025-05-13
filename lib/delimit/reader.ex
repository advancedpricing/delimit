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
      # Normalize line endings
      normalized_string = normalize_line_endings(string)

      # Split into lines
      all_lines = String.split(normalized_string, "\n")

      # Get delimiter
      delimiter = Keyword.get(options, :delimiter, ",")
      
      # Apply skip options and filter comment lines
      lines_after_skip = apply_skip_options(all_lines, options)

      # Filter out comment lines
      valid_lines = Enum.reject(lines_after_skip, fn line ->
        trimmed = String.trim(line)
        trimmed == "" || String.starts_with?(trimmed, "#")
      end)
      
      # Handle empty result
      if length(valid_lines) == 0 do
        []
      else
        # Check if headers are enabled
        has_headers = Keyword.get(options, :headers, true)
        
        if has_headers do
          # Extract header line
          [header_line | data_lines] = valid_lines
          
          # Parse headers
          headers = String.split(header_line, delimiter) |> Enum.map(&String.trim/1)
          
          # Cache header positions for better performance
          header_positions = cache_header_positions(schema, headers)
          
          # Parse each data line
          Enum.map(data_lines, fn line ->
            if String.trim(line) == "" do
              nil
            else
              # Parse values
              values = String.split(line, delimiter)
              
              # Convert to struct
              Schema.to_struct(schema, values, headers, header_positions)
            end
          end)
          |> Enum.reject(&is_nil/1) # Filter out nil results
        else
          # No headers, all lines are data
          Enum.map(valid_lines, fn line ->
            if String.trim(line) == "" do
              nil
            else
              values = String.split(line, delimiter)
              Schema.to_struct(schema, values, nil)
            end
          end)
          |> Enum.reject(&is_nil/1)
        end
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
    unless File.exists?(path) do
      raise "File does not exist: #{path}"
    end

    # Get key options
    has_headers = Keyword.get(options, :headers, true)
    delimiter = Keyword.get(options, :delimiter, ",")
    skip_lines = Keyword.get(options, :skip_lines, 0)

    if has_headers do
      # Need to read the file first to get headers
      case File.read(path) do
        {:ok, content} ->
          # Normalize line endings
          content = normalize_line_endings(content)
          
          # Split into lines
          all_lines = String.split(content, "\n")
          
          # Apply skip options
          lines_after_skip = apply_skip_options(all_lines, options)
          
          # Filter out comment lines
          valid_lines = Enum.reject(lines_after_skip, fn line ->
            trimmed = String.trim(line)
            trimmed == "" || String.starts_with?(trimmed, "#")
          end)
          
          # Check if we have any valid lines
          if length(valid_lines) == 0 do
            Stream.map([], & &1)
          else
            # Extract headers from first line
            [header_line | _] = valid_lines
            headers = String.split(header_line, delimiter) |> Enum.map(&String.trim/1)
            
            # Cache header positions
            header_positions = cache_header_positions(schema, headers)
            
            # Calculate total lines to skip
            total_skip = skip_lines + (if has_headers, do: 1, else: 0)
            
            # Stream from file
            path
            |> File.stream!()
            |> Stream.map(&normalize_line_endings/1)
            |> Stream.filter(fn line -> 
              trimmed = String.trim(line)
              trimmed != "" && !String.starts_with?(trimmed, "#")
            end)
            |> Stream.drop(total_skip)
            |> Stream.map(fn line ->
              values = String.split(line, delimiter)
              Schema.to_struct(schema, values, headers, header_positions)
            end)
          end
        
        {:error, reason} ->
          raise "Failed to read file: #{reason}"
      end
    else
      # No headers - simpler processing
      path
      |> File.stream!()
      |> Stream.map(&normalize_line_endings/1)
      |> Stream.filter(fn line -> 
        trimmed = String.trim(line)
        trimmed != "" && !String.starts_with?(trimmed, "#")
      end)
      |> Stream.drop(skip_lines)
      |> Stream.map(fn line ->
        values = String.split(line, delimiter)
        Schema.to_struct(schema, values, nil)
      end)
    end
  end

  # Normalize line endings to \n
  defp normalize_line_endings(string) do
    if String.contains?(string, "\r\n") do
      String.replace(string, "\r\n", "\n")
    else
      string
    end
  end

  # Apply skip options to lines
  defp apply_skip_options(lines, options) do
    # Apply skip_lines option
    lines_after_skip_lines = 
      case Keyword.get(options, :skip_lines, 0) do
        0 -> lines
        n -> Enum.drop(lines, n)
      end
    
    # Apply skip_while option
    case Keyword.get(options, :skip_while) do
      nil -> lines_after_skip_lines
      skip_fn -> Enum.drop_while(lines_after_skip_lines, skip_fn)
    end
  end

  # Cache header positions for better field lookup performance
  defp cache_header_positions(%Schema{} = schema, headers) do
    # Get non-embed fields
    fields = Enum.filter(schema.fields, fn field -> field.type != :embed end)
    
    # Map each field to its position in the headers
    Enum.reduce(fields, %{}, fn field, acc ->
      # Get header name from label option or field name
      header_name = field.opts[:label] || Atom.to_string(field.name)
      
      # Find position in the headers
      position = Enum.find_index(headers, fn h -> h == header_name end)
      
      # Only add to map if position was found
      if position != nil do
        Map.put(acc, field.name, position)
      else
        acc
      end
    end)
  end
end