defmodule Delimit.FixedWidth.Reader do
  @moduledoc """
  Functions for reading fixed-width data from files or strings.

  This module handles parsing of fixed-width format data where each
  field occupies a specific number of characters per line.
  """

  alias Delimit.Schema

  @doc """
  Reads fixed-width data from a string.

  ## Parameters

    * `schema` - The schema definition
    * `string` - String containing fixed-width data
    * `opts` - Read options

  ## Returns

    * List of structs with parsed data based on schema
  """
  @spec read_string(Schema.t(), binary(), Keyword.t()) :: [struct()]
  def read_string(%Schema{} = schema, string, opts \\ []) do
    Schema.validate_fixed_width!(schema)

    if string == "" do
      []
    else
      widths = Schema.field_widths(schema)
      skip_lines = Keyword.get(opts, :skip_lines, 0)
      skip_while_fn = Keyword.get(opts, :skip_while)
      headers_enabled = Keyword.get(opts, :headers, false)

      lines = split_lines(string)

      lines =
        if skip_while_fn, do: Enum.reject(lines, skip_while_fn), else: lines

      lines = if headers_enabled, do: Enum.drop(lines, 1), else: lines
      lines = if skip_lines > 0, do: Enum.drop(lines, skip_lines), else: lines

      lines
      |> Enum.reject(fn line -> line == "" end)
      |> Enum.map(fn line ->
        values = slice_line(line, widths)
        Schema.to_struct_from_flat_values(schema, values, Keyword.take(opts, [:trim_fields]))
      end)
    end
  end

  @doc """
  Reads fixed-width data from a file.

  ## Parameters

    * `schema` - The schema definition
    * `path` - Path to the fixed-width file
    * `opts` - Read options

  ## Returns

    * List of structs with parsed data based on schema
  """
  @spec read_file(Schema.t(), Path.t(), Keyword.t()) :: [struct()]
  def read_file(%Schema{} = schema, path, opts \\ []) do
    case File.read(path) do
      {:ok, content} ->
        read_string(schema, content, opts)

      {:error, reason} ->
        raise "Failed to read file: #{reason}"
    end
  end

  @doc """
  Streams fixed-width data from a file.

  ## Parameters

    * `schema` - The schema definition
    * `path` - Path to the fixed-width file
    * `opts` - Read options

  ## Returns

    * Stream of structs with parsed data based on schema
  """
  @spec stream_file(Schema.t(), Path.t(), Keyword.t()) :: Enumerable.t()
  def stream_file(%Schema{} = schema, path, opts \\ []) do
    Schema.validate_fixed_width!(schema)

    if !File.exists?(path) do
      raise "File does not exist: #{path}"
    end

    widths = Schema.field_widths(schema)
    skip_lines = Keyword.get(opts, :skip_lines, 0)
    skip_while_fn = Keyword.get(opts, :skip_while)
    headers_enabled = Keyword.get(opts, :headers, false)

    path
    |> File.stream!()
    |> Stream.map(&String.trim_trailing(&1, "\n"))
    |> Stream.map(&String.trim_trailing(&1, "\r"))
    |> maybe_skip_while(skip_while_fn)
    |> maybe_skip_headers(headers_enabled)
    |> maybe_skip_lines(skip_lines)
    |> Stream.reject(fn line -> line == "" end)
    |> Stream.map(fn line ->
      values = slice_line(line, widths)
      Schema.to_struct_from_flat_values(schema, values, Keyword.take(opts, [:trim_fields]))
    end)
  end

  # Split input string into lines, handling both \r\n and \n
  defp split_lines(string) do
    String.split(string, ~r/\r?\n/)
  end

  # Slice a line into field values based on field widths
  defp slice_line(line, widths) do
    line_length = String.length(line)

    Enum.map(widths, fn {_field, offset, width} ->
      if offset >= line_length do
        nil
      else
        available = min(width, line_length - offset)
        String.slice(line, offset, available)
      end
    end)
  end

  defp maybe_skip_while(stream, nil), do: stream
  defp maybe_skip_while(stream, skip_fn), do: Stream.reject(stream, skip_fn)

  defp maybe_skip_headers(stream, false), do: stream
  defp maybe_skip_headers(stream, true), do: Stream.drop(stream, 1)

  defp maybe_skip_lines(stream, 0), do: stream
  defp maybe_skip_lines(stream, n), do: Stream.drop(stream, n)
end
