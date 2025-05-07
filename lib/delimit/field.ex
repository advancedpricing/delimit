defmodule Delimit.Field do
  @moduledoc """
  Defines field types, options, and conversion functions for Delimit schemas.

  This module handles field definitions, data type conversions, and validation
  for delimited data format parsing and generation.
  """

  @typedoc """
  Field definition structure.

  * `:name` - The name of the field
  * `:type` - The data type of the field
  * `:opts` - Additional options for the field
  """
  @type t :: %__MODULE__{
          name: atom(),
          type: atom(),
          opts: Keyword.t()
        }

  @enforce_keys [:name, :type]
  defstruct [:name, :type, opts: []]

  @typedoc """
  Supported field types.
  """
  @type field_type ::
          :string
          | :integer
          | :float
          | :boolean
          | :date
          | :datetime
          | :embed

  @typedoc """
  Boolean field configuration options.
  """
  @type boolean_opts :: [
          true_values: [String.t()],
          false_values: [String.t()]
        ]

  @typedoc """
  Date field configuration options.
  """
  @type date_opts :: [
          format: String.t()
        ]

  @typedoc """
  General field configuration options.
  """
  @type field_opts :: [
          optional: boolean(),
          default: any(),
          read_fn: (String.t() -> any()),
          write_fn: (any() -> String.t()),
          nil_on_empty: boolean()
          | boolean_opts()
          | date_opts()
        ]

  @doc """
  Creates a new field definition.

  ## Parameters

    * `name` - The name of the field as an atom
    * `type` - The type of the field (`:string`, `:integer`, etc.)
    * `opts` - A keyword list of options for the field

  ## Example

      iex> Delimit.Field.new(:first_name, :string, [])
      %Delimit.Field{name: :first_name, type: :string, opts: []}

      iex> Delimit.Field.new(:age, :integer, [default: 0])
      %Delimit.Field{name: :age, type: :integer, opts: [default: 0]}
  """
  @spec new(atom(), field_type(), field_opts()) :: t()
  def new(name, type, opts \\ []) when is_atom(name) and is_atom(type) and is_list(opts) do
    # Validate the field type
    unless type in [:string, :integer, :float, :boolean, :date, :datetime, :embed] do
      raise ArgumentError, "Unsupported field type: #{inspect(type)}"
    end

    %__MODULE__{name: name, type: type, opts: opts}
  end

  @doc """
  Parses a raw string value into the specified type.

  ## Parameters

    * `value` - The raw string value from the delimited file
    * `field` - The field definition

  ## Returns

    * The parsed value or nil if the value is empty and nil_on_empty is true

  ## Example

      iex> field = Delimit.Field.new(:age, :integer)
      iex> Delimit.Field.parse_value("42", field)
      42

      iex> field = Delimit.Field.new(:active, :boolean)
      iex> Delimit.Field.parse_value("Yes", field)
      true
  """
  @spec parse_value(String.t() | nil, t()) :: any()
  def parse_value(value, field) do
    # If value is nil, return nil
    if is_nil(value) do
      field.opts[:default]
    else
      value = if field.opts[:trim] != false, do: String.trim(value), else: value

      # Handle empty strings
      if value == "" and field.opts[:nil_on_empty] != false do
        field.opts[:default]
      else
        # If a custom read function is provided, use it
        if read_fn = field.opts[:read_fn] do
          read_fn.(value)
        else
          do_parse_value(value, field)
        end
      end
    end
  end

  # Type-specific parsing functions
  defp do_parse_value(value, %__MODULE__{type: :string}) do
    value
  end

  defp do_parse_value(value, %__MODULE__{type: :integer}) do
    case Integer.parse(value) do
      {integer, _} -> integer
      :error -> raise "Cannot convert '#{value}' to integer"
    end
  end

  defp do_parse_value(value, %__MODULE__{type: :float}) do
    case Float.parse(value) do
      {float, _} -> float
      :error -> raise "Cannot convert '#{value}' to float"
    end
  end

  defp do_parse_value(value, %__MODULE__{type: :boolean} = field) do
    true_values = field.opts[:true_values] || ["true", "yes", "y", "1", "t", "on"]
    false_values = field.opts[:false_values] || ["false", "no", "n", "0", "f", "off"]

    downcased = String.downcase(value)

    cond do
      Enum.member?(true_values, downcased) -> true
      Enum.member?(false_values, downcased) -> false
      true -> raise "Cannot convert '#{value}' to boolean"
    end
  end

  defp do_parse_value(value, %__MODULE__{type: :date} = field) do
    format = field.opts[:format] || "{YYYY}-{0M}-{0D}"

    # Use Date.from_iso8601 for standard ISO dates to avoid Timex warnings
    # when possible
    result = if format == "{YYYY}-{0M}-{0D}" do
      Date.from_iso8601(value)
    else
      case Timex.parse(value, format) do
        {:ok, date} -> {:ok, date}
        {:error, reason} -> {:error, reason}
      end
    end

    case result do
      {:ok, date} -> date
      {:error, reason} -> raise "Cannot convert '#{value}' to date: #{reason}"
    end
  end

  defp do_parse_value(value, %__MODULE__{type: :datetime} = field) do
    format = field.opts[:format] || "{ISO:Extended}"

    # Use DateTime.from_iso8601 for standard ISO dates to avoid Timex warnings
    # when possible
    result = if format == "{ISO:Extended}" do
      case DateTime.from_iso8601(value) do
        {:ok, datetime, _offset} -> {:ok, datetime}
        error -> error
      end
    else
      case Timex.parse(value, format) do
        {:ok, datetime} -> {:ok, datetime}
        {:error, reason} -> {:error, reason}
      end
    end

    case result do
      {:ok, datetime} -> datetime
      {:error, reason} -> raise "Cannot convert '#{value}' to datetime: #{reason}"
    end
  end

  @doc """
  Converts a value to a string representation for writing to a delimited file.

  ## Parameters

    * `value` - The value to convert
    * `field` - The field definition

  ## Returns

    * The string representation of the value

  ## Example

      iex> field = Delimit.Field.new(:age, :integer)
      iex> Delimit.Field.to_string(42, field)
      "42"

      iex> field = Delimit.Field.new(:active, :boolean)
      iex> Delimit.Field.to_string(true, field)
      "true"
  """
  @spec to_string(any(), t()) :: String.t()
  def to_string(value, field) do
    # If value is nil, return empty string
    if is_nil(value) do
      ""
    else
      # If a custom write function is provided, use it
      if write_fn = field.opts[:write_fn] do
        write_fn.(value)
      else
        do_to_string(value, field)
      end
    end
  end

  # Type-specific conversion to string functions
  defp do_to_string(value, %__MODULE__{type: :string}) when is_binary(value) do
    value
  end

  defp do_to_string(value, %__MODULE__{type: :integer}) when is_integer(value) do
    Integer.to_string(value)
  end

  defp do_to_string(value, %__MODULE__{type: :float}) when is_float(value) do
    Float.to_string(value)
  end

  defp do_to_string(value, %__MODULE__{type: :boolean} = field) when is_boolean(value) do
    if value do
      field.opts[:true_value] || "true"
    else
      field.opts[:false_value] || "false"
    end
  end

  defp do_to_string(value, %__MODULE__{type: :date} = field) do
    format = field.opts[:format] || "{YYYY}-{0M}-{0D}"
    
    # Use Date.to_iso8601 for standard ISO dates to avoid Timex warnings
    if format == "{YYYY}-{0M}-{0D}" do
      Date.to_iso8601(value)
    else
      Timex.format!(value, format)
    end
  end

  defp do_to_string(value, %__MODULE__{type: :datetime} = field) do
    format = field.opts[:format] || "{ISO:Extended}"
    
    # Use DateTime.to_iso8601 for standard ISO dates to avoid Timex warnings
    if format == "{ISO:Extended}" do
      DateTime.to_iso8601(value)
    else
      Timex.format!(value, format)
    end
  end

  defp do_to_string(value, _field) do
    Kernel.to_string(value)
  end
end