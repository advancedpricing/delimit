defmodule Delimit.FixedWidth.Writer do
  @moduledoc """
  Functions for writing fixed-width data to files or strings.

  This module handles serialization of structs into fixed-width format
  where each field occupies a specific number of characters.
  """

  alias Delimit.Schema

  @doc """
  Writes fixed-width data to a string.

  ## Parameters

    * `schema` - The schema definition
    * `data` - List of structs to write
    * `opts` - Write options (`:line_ending`, `:headers`)

  ## Returns

    * String containing the fixed-width data
  """
  @spec write_string(Schema.t(), [struct()], Keyword.t()) :: binary()
  def write_string(%Schema{} = schema, data, opts \\ []) do
    Schema.validate_fixed_width!(schema)

    line_ending = Keyword.get(opts, :line_ending, "\r\n")
    all_fields = Schema.collect_all_fields(schema)

    rows =
      Enum.map(data, fn item ->
        values = Schema.to_row(schema, item)

        values
        |> Enum.zip(all_fields)
        |> Enum.map_join(fn {value, {_name, field}} ->
          format_field(value, field)
        end)
      end)

    case rows do
      [] -> ""
      _ -> Enum.join(rows, line_ending) <> line_ending
    end
  end

  @doc """
  Writes fixed-width data to a file.

  ## Parameters

    * `schema` - The schema definition
    * `path` - Path to the output file
    * `data` - List of structs to write
    * `opts` - Write options

  ## Returns

    * `:ok` on success
  """
  @spec write_file(Schema.t(), Path.t(), [struct()], Keyword.t()) :: :ok
  def write_file(%Schema{} = schema, path, data, opts \\ []) do
    content = write_string(schema, data, opts)
    File.write!(path, content)
  end

  @doc """
  Streams fixed-width data to a file.

  ## Parameters

    * `schema` - The schema definition
    * `path` - Path to the output file
    * `data_stream` - Stream of structs to write
    * `opts` - Write options

  ## Returns

    * `:ok` on success
  """
  @spec stream_to_file(Schema.t(), Path.t(), Enumerable.t(), Keyword.t()) :: :ok
  def stream_to_file(%Schema{} = schema, path, data_stream, opts \\ []) do
    Schema.validate_fixed_width!(schema)

    line_ending = Keyword.get(opts, :line_ending, "\r\n")
    all_fields = Schema.collect_all_fields(schema)

    path_string = if is_binary(path), do: path, else: to_string(path)
    temp_path = path_string <> ".tmp"

    try do
      {:ok, file} = File.open(temp_path, [:write, :utf8])

      data_stream
      |> Enum.chunk_every(100)
      |> Enum.each(fn chunk ->
        iodata =
          Enum.map(chunk, fn item ->
            values = Schema.to_row(schema, item)

            line =
              values
              |> Enum.zip(all_fields)
              |> Enum.map_join(fn {value, {_name, field}} ->
                format_field(value, field)
              end)

            [line, line_ending]
          end)

        IO.binwrite(file, iodata)
      end)

      File.close(file)
      File.rename(temp_path, path_string)
      :ok
    after
      File.rm(temp_path)
    end
  end

  defp format_field(value, field) do
    width = Keyword.fetch!(field.opts, :width)
    pad_char = Keyword.get(field.opts, :pad_char, " ")
    justify = Keyword.get(field.opts, :justify, :left)

    str = if value == "" or is_nil(value), do: "", else: value

    case_result =
      case justify do
        :right -> String.pad_leading(str, width, pad_char)
        _ -> String.pad_trailing(str, width, pad_char)
      end

    String.slice(case_result, 0, width)
  end
end
