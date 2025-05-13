defmodule NimbleCsvTest do
  use ExUnit.Case, async: true
  
  # Define parsers at the module level (compile time) instead of in the test (runtime)
  require NimbleCSV
  NimbleCSV.define(DoubleQuoteParser, separator: ",")
  NimbleCSV.define(SingleQuoteParser, separator: ",", escape: "'")
  
  test "NimbleCSV supports custom escape character" do    
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
    
    parsed_single_quote = SingleQuoteParser.parse_string(clean_single_quote_csv)
    assert length(parsed_single_quote) == 1
    assert Enum.at(parsed_single_quote, 0) == ["1", "John, Doe", "This is a description, with commas"]
  end
  
  test "round trip with custom escape character" do
    # Test data with values that need escaping
    original_data = [
      ["id", "name", "description"],
      ["1", "Test, with comma", "description, with comma"]
    ]
    
    # Write to CSV string
    csv = SingleQuoteParser.dump_to_iodata(original_data) |> IO.iodata_to_binary()
    
    # Verify proper escaping is used
    assert String.contains?(csv, "'Test, with comma'")
    
    # Parse the CSV back to data
    parsed_data = SingleQuoteParser.parse_string(csv)
    
    # Should match the data (without headers since NimbleCSV parses only data rows)
    assert parsed_data == [Enum.at(original_data, 1)]
  end
end