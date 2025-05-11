defmodule NimbleCsvTest do
  use ExUnit.Case, async: true
  
  test "NimbleCSV supports custom escape character" do
    # Define parsers with different escape characters in the test
    require NimbleCSV
    NimbleCSV.define(DoubleQuoteParser, separator: ",")
    # Create unique module name to avoid conflicts
    single_quote_parser_name = String.to_atom("SingleQuoteParser_#{System.unique_integer([:positive])}")
    NimbleCSV.define(single_quote_parser_name, separator: ",", escape: "'")
    
    # Test parsing with double quotes as escape character
    double_quote_csv = """
    id,name,description
    1,"John, Doe","This is a description, with commas"
    """
    
    # Clean up any potential hidden whitespace or other characters
    clean_double_quote_csv = String.trim(double_quote_csv)
    
    parsed_double_quote = DoubleQuoteParser.parse_string(clean_double_quote_csv)

    assert length(parsed_double_quote) == 1
    assert Enum.at(parsed_double_quote, 0) == ["1", "John, Doe", "This is a description, with commas"]
    
    # Test parsing with single quotes as escape character
    single_quote_csv = """
    id,name,description
    1,'John, Doe','This is a description, with commas'
    """
    
    # Clean up any potential hidden whitespace or other characters
    clean_single_quote_csv = String.trim(single_quote_csv)
    
    parsed_single_quote = single_quote_parser_name.parse_string(clean_single_quote_csv)
    assert length(parsed_single_quote) == 1
    assert Enum.at(parsed_single_quote, 0) == ["1", "John, Doe", "This is a description, with commas"]
  end
  
  test "round trip with custom escape character" do
    # Define parser with quote escaping
    require NimbleCSV
    unique_module_name = String.to_atom("RoundTripParser_#{System.unique_integer([:positive])}")
    NimbleCSV.define(unique_module_name, separator: ",", escape: "'")
    
    # Test data with values that need escaping
    original_data = [
      ["id", "name", "description"],
      ["1", "Test, with comma", "description, with comma"]
    ]
    
    # Write to CSV string
    csv = unique_module_name.dump_to_iodata(original_data) |> IO.iodata_to_binary()
    
    # Verify proper escaping is used
    assert String.contains?(csv, "'Test, with comma'")
    
    # Parse the CSV back to data
    parsed_data = unique_module_name.parse_string(csv)
    
    # Should match the data (without headers since NimbleCSV parses only data rows)
    assert parsed_data == [Enum.at(original_data, 1)]
  end
end