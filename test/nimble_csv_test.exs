defmodule NimbleCsvTest do
  use ExUnit.Case, async: true
  
  test "NimbleCSV supports custom escape character" do
    # Define parsers with different escape characters in the test
    require NimbleCSV
    NimbleCSV.define(DoubleQuoteParser, separator: ",")
    NimbleCSV.define(SingleQuoteParser, separator: ",", escape: "'")
    
    # Test parsing with double quotes as escape character
    double_quote_csv = """
    id,name,description
    1,"John, Doe","This is a description, with commas"
    """
    
    # Print the exact CSV string
    IO.puts("\nDouble quote CSV string:")
    IO.puts(double_quote_csv)
    IO.puts("CSV length: #{String.length(double_quote_csv)}")
    IO.inspect(String.codepoints(double_quote_csv), limit: :infinity, label: "Characters")
    
    # Clean up any potential hidden whitespace or other characters
    clean_double_quote_csv = String.trim(double_quote_csv)
    
    parsed_double_quote = DoubleQuoteParser.parse_string(clean_double_quote_csv)
    IO.inspect(parsed_double_quote, label: "Parsed double quote CSV")
    assert length(parsed_double_quote) == 1
    assert Enum.at(parsed_double_quote, 0) == ["1", "John, Doe", "This is a description, with commas"]
    
    # Test parsing with single quotes as escape character
    single_quote_csv = """
    id,name,description
    1,'John, Doe','This is a description, with commas'
    """
    
    # Print the exact CSV string
    IO.puts("\nSingle quote CSV string:")
    IO.puts(single_quote_csv)
    
    # Clean up any potential hidden whitespace or other characters
    clean_single_quote_csv = String.trim(single_quote_csv)
    
    parsed_single_quote = SingleQuoteParser.parse_string(clean_single_quote_csv)
    IO.inspect(parsed_single_quote, label: "Parsed single quote CSV")
    assert length(parsed_single_quote) == 1
    assert Enum.at(parsed_single_quote, 0) == ["1", "John, Doe", "This is a description, with commas"]
  end
  
  test "round trip with custom escape character" do
    require NimbleCSV
    NimbleCSV.define(CustomParser, separator: ",", escape: "'")
    
    # Test data with values that need escaping
    original_data = [
      ["id", "name", "description"],
      ["1", "Test, with comma", "description, with comma"]
    ]
    
    # Write to CSV string
    csv = CustomParser.dump_to_iodata(original_data) |> IO.iodata_to_binary()
    
    # Print the generated CSV string
    IO.puts("\nGenerated CSV with custom escape:")
    IO.puts(csv)
    IO.inspect(String.codepoints(csv), limit: :infinity, label: "CSV characters")
    
    # Verify proper escaping is used
    assert String.contains?(csv, "'Test, with comma'")
    
    # Parse the CSV back to data
    parsed_data = CustomParser.parse_string(csv)
    IO.inspect(parsed_data, label: "Parsed data")
    IO.inspect(original_data, label: "Original data")
    
    # Should match the data (without headers since NimbleCSV parses only data rows)
    assert parsed_data == [Enum.at(original_data, 1)]
  end
end