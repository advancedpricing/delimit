defmodule Delimit.Parsers do
  @moduledoc """
  Pre-defined CSV parsers for common delimiters.

  This module defines optimized parsers for common delimiters to avoid
  creating new parser modules for each parsing operation.
  """

  # Create parsers for common delimiters
  NimbleCSV.define(DelimitCommaParser, separator: ",")
  NimbleCSV.define(DelimitTabParser, separator: "\t")
  NimbleCSV.define(DelimitSemicolonParser, separator: ";")
  NimbleCSV.define(DelimitPipeParser, separator: "|")

  @doc """
  Gets the appropriate parser for the given delimiter.

  ## Parameters

    * `delimiter` - The delimiter character (comma, tab, etc.)

  ## Returns

    * A module that implements NimbleCSV parser functions

  ## Examples

      iex> Delimit.Parsers.get_parser(",")
      DelimitCommaParser

      iex> Delimit.Parsers.get_parser(";")
      DelimitSemicolonParser
  """
  @spec get_parser(String.t()) :: module()
  def get_parser(delimiter) do
    case delimiter do
      "," -> DelimitCommaParser
      "\t" -> DelimitTabParser
      ";" -> DelimitSemicolonParser
      "|" -> DelimitPipeParser
      _ ->
        # For custom delimiters, create a unique parser
        unique_module_name =
          String.to_atom("DelimitDynamicParser_#{System.unique_integer([:positive])}")

        NimbleCSV.define(unique_module_name, separator: delimiter)
        unique_module_name
    end
  end
end