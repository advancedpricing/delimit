defmodule Delimit.Parsers do
  @moduledoc """
  Pre-defined CSV parsers for common delimiters.

  This module defines optimized parsers for common delimiters to avoid
  creating new parser modules for each parsing operation.
  """

  # Create parsers for common delimiters with default escape (double quote)
  NimbleCSV.define(DelimitCommaParser, separator: ",")
  NimbleCSV.define(DelimitTabParser, separator: "\t")
  NimbleCSV.define(DelimitSemicolonParser, separator: ";")
  NimbleCSV.define(DelimitPipeParser, separator: "|")

  @doc """
  Gets the appropriate parser for the given delimiter.

  ## Parameters

    * `delimiter` - The delimiter character (comma, tab, etc.)
    * `opts` - Parser options (reserved for future use)

  ## Returns

    * A module that implements NimbleCSV parser functions

  ## Examples

      iex> Delimit.Parsers.get_parser(",")
      DelimitCommaParser

      iex> Delimit.Parsers.get_parser(";")
      DelimitSemicolonParser
  """
  @spec get_parser(String.t(), Keyword.t()) :: module()
  def get_parser(delimiter, _opts \\ []) do
    case delimiter do
      "," -> DelimitCommaParser
      "\t" -> DelimitTabParser
      ";" -> DelimitSemicolonParser
      "|" -> DelimitPipeParser
      _ ->
        # For custom delimiters, create a unique parser
        unique_module_name =
          String.to_atom("DelimitDynamicParser_#{System.unique_integer([:positive])}")

        parser_opts = [separator: delimiter]
        
        NimbleCSV.define(unique_module_name, parser_opts)
        unique_module_name
    end
  end
  
  @doc """
  Gets a parser with custom escape character.

  ## Parameters

    * `delimiter` - The delimiter character (comma, tab, etc.)
    * `escape` - The escape character (default: double-quote)
    * `opts` - Additional options (reserved for future use)

  ## Returns

    * A module that implements NimbleCSV parser functions

  ## Examples

      iex> Delimit.Parsers.get_parser_with_escape(",", "'")
      # Returns a dynamically generated parser module
  """
  @spec get_parser_with_escape(String.t(), String.t(), Keyword.t()) :: module()
  def get_parser_with_escape(delimiter, escape, _opts \\ []) do
    # Always create a custom parser with the specified escape character
    unique_module_name =
      String.to_atom("DelimitEscapeParser_#{System.unique_integer([:positive])}")
    
    parser_opts = [
      separator: delimiter,
      escape: escape
    ]

    NimbleCSV.define(unique_module_name, parser_opts)
    unique_module_name
  end
end