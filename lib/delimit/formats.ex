defmodule Delimit.Formats do
  @moduledoc """
  Standard format configurations for common delimited file types.

  This module provides predefined format configurations for common file formats
  such as CSV (comma-separated values), TSV (tab-separated values),
  PSV (pipe-separated values), and SSV (semi-colon separated values).

  Using these formats simplifies configuration by setting appropriate defaults
  for delimiter, escape character, and other format-specific options.
  """

  @doc """
  Returns configuration options for a specified file format.

  ## Parameters

    * `format` - The format identifier (`:csv`, `:tsv`, `:psv`)

  ## Returns

    * A keyword list of options appropriate for the specified format

  ## Examples

      iex> Delimit.Formats.get_options(:csv)
      [delimiter: ",", escape: "\""]

      iex> Delimit.Formats.get_options(:tsv)
      [delimiter: "\\t", escape: "\""]
  """
  @spec get_options(atom()) :: Keyword.t()
  def get_options(format) do
    case format do
      :csv -> [delimiter: ",", escape: "\""]
      :tsv -> [delimiter: "\t", escape: "\""]
      :psv -> [delimiter: "|", escape: "\""]
      :ssv -> [delimiter: ";", escape: "\""]
      _ -> raise ArgumentError, "Unsupported format: #{inspect(format)}"
    end
  end

  @doc """
  Lists all supported format identifiers.

  ## Returns

    * A list of supported format atoms

  ## Examples

      iex> Delimit.Formats.supported_formats()
      [:csv, :tsv, :psv, :ssv]
  """
  @spec supported_formats() :: [atom()]
  def supported_formats do
    [:csv, :tsv, :psv, :ssv]
  end

  @doc """
  Merges format options with custom options.

  Format options take precedence over schema defaults but are overridden by
  explicitly provided custom options.

  ## Parameters

    * `schema_options` - Base options from the schema
    * `format` - The format identifier, or nil for no format
    * `custom_options` - Custom options that will override format options

  ## Returns

    * A keyword list of merged options

  ## Examples

      iex> schema_opts = [trim_fields: false]
      iex> Delimit.Formats.merge_options(schema_opts, :csv, [escape: "'"])
      [trim_fields: false, delimiter: ",", escape: "'"]
  """
  @spec merge_options(Keyword.t(), atom() | nil, Keyword.t()) :: Keyword.t()
  def merge_options(schema_options, format, custom_options) do
    options =
      case format do
        nil ->
          Keyword.merge(schema_options, custom_options)

        format ->
          format_options = get_options(format)

          schema_options
          |> Keyword.merge(format_options)
          |> Keyword.merge(custom_options)
      end

    # Special handling for format-specific options
    options =
      case format do
        :tsv ->
          # For TSV, ensure format is preserved
          Keyword.put_new(options, :delimiter, "\t")

        _ ->
          options
      end

    options
  end
end
