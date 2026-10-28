defmodule Delimit.HeaderError do
  @moduledoc """
  Raised when a file read with `validate_headers: true` does not have the header
  its schema expects.

  ## Fields

    * `:expected` - The headers the schema expects, in order
    * `:found` - The headers in the file, in order (empty when the file has none)
    * `:missing` - Expected headers that are not in the file
    * `:unexpected` - Headers in the file that the schema does not expect

  `missing` and `unexpected` are both empty when the file has the right headers
  in the wrong order.
  """

  defexception [:expected, :found, :missing, :unexpected]

  @type t :: %__MODULE__{
          expected: [String.t()],
          found: [String.t()],
          missing: [String.t()],
          unexpected: [String.t()]
        }

  @byte_order_mark "\uFEFF"

  @doc """
  Compare the header row of a file with the headers a schema expects.

  Headers match ignoring case and surrounding whitespace, and a UTF-8 byte order
  mark before the first header is ignored. The order must match, as columns are
  read by position when streaming.

  ## Example

      iex> Delimit.HeaderError.check(["name", "age"], ["Name ", "AGE"])
      :ok

      iex> {:error, error} = Delimit.HeaderError.check(["name", "age"], ["name"])
      iex> error.missing
      ["age"]
  """
  @spec check([String.t()], [String.t()]) :: :ok | {:error, t()}
  def check(expected, found) when is_list(expected) and is_list(found) do
    found = strip_byte_order_mark(found)

    if normalize(found) == normalize(expected) do
      :ok
    else
      {:error,
       %__MODULE__{
         expected: expected,
         found: found,
         missing: difference(expected, found),
         unexpected: difference(found, expected)
       }}
    end
  end

  @impl true
  def message(%__MODULE__{} = error) do
    found =
      if error.found == [],
        do: "no header",
        else: "the columns " <> Enum.join(error.found, ", ")

    details =
      [{"missing", error.missing}, {"unexpected", error.unexpected}]
      |> Enum.reject(fn {_label, headers} -> headers == [] end)
      |> Enum.map_join("; ", fn {label, headers} -> "#{label}: #{Enum.join(headers, ", ")}" end)

    message = "expected the columns #{Enum.join(error.expected, ", ")} but found #{found}"

    if details == "", do: message, else: "#{message} (#{details})"
  end

  defp strip_byte_order_mark([first | rest]),
    do: [String.trim_leading(first, @byte_order_mark) | rest]

  defp strip_byte_order_mark([]), do: []

  defp normalize(headers), do: Enum.map(headers, &normalize_header/1)

  defp normalize_header(header), do: header |> String.trim() |> String.downcase()

  # Headers in `left` with no match in `right`, as written in `left`.
  defp difference(left, right) do
    right = MapSet.new(right, &normalize_header/1)
    Enum.reject(left, &MapSet.member?(right, normalize_header(&1)))
  end
end
