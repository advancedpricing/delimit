defmodule Delimit.ReaderTest do
  use ExUnit.Case, async: true

  alias Delimit.TestSupport.Helpers
  alias Delimit.TestSupport.Schemas.CustomBoolean
  alias Delimit.TestSupport.Schemas.CustomConversion
  alias Delimit.TestSupport.Schemas.FullSchema
  alias Delimit.TestSupport.Schemas.SchemaWithDefaults
  alias Delimit.TestSupport.Schemas.TrimSchema

  describe "basic reading functionality" do
    test "reads CSV with headers" do
      csv_string = Helpers.sample_csv_with_headers()

      # Read with headers option
      people = FullSchema.read_string(csv_string, headers: true)
      assert length(people) == 3

      # Verify data was read correctly
      person = Enum.at(people, 0)
      assert person.first_name == "John"
      assert person.last_name == "Doe"
      assert person.age == 30
      assert person.salary == 50_000.50
      assert person.active == true
      assert person.notes == "Good employee"
    end

    test "reads CSV without headers" do
      # CSV without headers
      csv_string = """
      John,Doe,30,50000.50,2020-01-15,true,Good employee
      Jane,Smith,28,55000.75,2019-05-20,true,
      Bob,Johnson,45,75000.00,2015-11-10,false,On probation
      """

      # Read without headers option
      people = FullSchema.read_string(csv_string)
      assert length(people) == 3

      # Verify data was read correctly
      person = Enum.at(people, 0)
      assert person.first_name == "John"
      assert person.last_name == "Doe"
      assert person.age == 30
      assert person.salary == 50_000.50
      assert person.active == true
      assert person.notes == "Good employee"
    end

    test "read from file" do
      # Create a temporary file
      test_file = Helpers.create_temp_file(Helpers.sample_csv_with_headers())

      # Read from file
      people = FullSchema.read(test_file, headers: true)
      assert length(people) == 3

      # Verify data was read correctly
      person = Enum.at(people, 0)
      assert person.first_name == "John"
      assert person.last_name == "Doe"
      assert person.age == 30
    end
  end

  describe "skipping options" do
    test "skip using skip_while then skip_lines" do
      csv_string = Helpers.sample_csv_with_comments()

      # Skip with skip_while and header
      people_with_skip_while =
        FullSchema.read_string(csv_string,
          skip_while: fn line -> String.starts_with?(line, "#") end,
          headers: true
        )
        
      # Skip directly using skip_lines (comment lines + header)
      people_with_skip_lines = FullSchema.read_string(csv_string, skip_lines: 3, headers: false)
      
      # Both approaches should return data
      assert length(people_with_skip_while) > 0
      assert length(people_with_skip_lines) > 0
      
      # Both approaches should give properly structured data
      person_with_skip_while = Enum.at(people_with_skip_while, 0)
      assert person_with_skip_while.first_name == "John"
      
      person_with_skip_lines = Enum.at(people_with_skip_lines, 0)
      assert is_map(person_with_skip_lines)
      assert Map.has_key?(person_with_skip_lines, :first_name)
      assert person_with_skip_lines.first_name == "John"
      
      # Testing specific case: first apply skip_while then skip_lines
      combined_skip = 
        FullSchema.read_string(csv_string,
          skip_while: fn line -> String.starts_with?(line, "#") end,
          skip_lines: 1,  # Skip the header line after skipping comments
          headers: false
        )
      
      assert length(combined_skip) > 0
      assert Enum.at(combined_skip, 0).first_name == "John"
    end
  end

  describe "empty and malformed data" do
    test "handles empty CSV file" do
      # Empty file
      people = FullSchema.read_string("")
      assert people == []
    end

    test "just whitespace" do
      # File with just whitespace
      people = FullSchema.read_string("   \r\n  \r\n")
      assert people == []
    end

    test "just a header" do
      # File with only a header line
      people =
        FullSchema.read_string("first_name,last_name,age,salary,hired_date,active,notes")

      # Since headers are treated as data by default, we expect one record
      assert length(people) == 1
    end

    test "CSV with missing columns" do
      # CSV with fewer columns than the schema
      csv_string = """
      John,Doe,30
      Jane,Smith
      """

      # Missing columns should be nil
      people = FullSchema.read_string(csv_string)
      assert length(people) == 2

      # First row has age but missing other fields
      person = Enum.at(people, 0)
      assert person.first_name == "John"
      assert person.last_name == "Doe"
      assert person.age == 30
      assert person.salary == nil

      # Second row is missing more fields
      person = Enum.at(people, 1)
      assert person.first_name == "Jane"
      assert person.last_name == "Smith"
      assert person.age == nil
    end

    test "CSV with extra columns" do
      # CSV with more columns than the schema
      csv_string = """
      John,Doe,30,50000.50,2020-01-15,true,Good employee,extra1,extra2
      """

      # Extra columns should be ignored
      people = FullSchema.read_string(csv_string)
      assert length(people) == 1

      person = Enum.at(people, 0)
      assert person.first_name == "John"
      assert person.last_name == "Doe"
      assert person.notes == "Good employee"
      # Extra columns aren't accessible
    end

    test "uses default values for missing data" do
      # CSV with missing data
      csv_string =
        ",,\r\n" <>
          "Name only,,"

      # Read with defaults schema
      items = SchemaWithDefaults.read_string(csv_string)

      assert length(items) == 2

      # First row uses all defaults
      item = Enum.at(items, 0)
      # default value
      assert item.name == "Unknown"
      # default value
      assert item.age == 0
      # default value
      assert item.active == false

      # Second row only has name
      item = Enum.at(items, 1)
      # provided value
      assert item.name == "Name only"
      # default value
      assert item.age == 0
      # default value
      assert item.active == false
    end

    test "row with only commas is considered an empty row with defaults" do
      # CSV with just commas (empty fields)
      csv_string = ",,"

      # Read with defaults schema
      items = SchemaWithDefaults.read_string(csv_string)

      assert length(items) == 1

      # Row should use all defaults
      item = Enum.at(items, 0)
      assert item.name == "Unknown"
      assert item.age == 0
      assert item.active == false
    end

    test "mixed empty and comma-only lines" do
      # CSV with mixed content:
      # 1. Row with data
      # 2. Empty line (no commas)
      # 3. Line with just commas (empty fields)
      # 4. Another row with data
      # 5. Empty trailing line
      csv_string = """
      John Doe,30,

      ,,
      Jane Smith,,

      """

      # Read with defaults schema
      items = SchemaWithDefaults.read_string(csv_string)
      
      # NimbleCSV treats each line as a row, including empty and partially empty ones.
      # This gives us 5 rows from the input:
      # 1. John Doe row
      # 2. Empty row with commas (defaults)
      # 3. Empty row (just blank line - defaults)
      # 4. Row with just commas (defaults)
      # 5. Jane Smith row
      assert length(items) == 5

      # First row should have name and age from data
      item = Enum.at(items, 0)
      assert item.name == "John Doe"
      assert item.age == 30
      # default
      assert item.active == false

      # Second row is the empty fields row (with commas) - should use defaults
      item = Enum.at(items, 1)
      # default
      assert item.name == "Unknown"
      # default
      assert item.age == 0
      # default
      assert item.active == false
      
      # Third row is the completely blank line - should use defaults
      item = Enum.at(items, 2)
      # default
      assert item.name == "Unknown"
      # default
      assert item.age == 0
      # default
      assert item.active == false

      # Fourth row has the "Jane Smith" data
      item = Enum.at(items, 3)
      assert item.name == "Jane Smith"
      # default
      assert item.age == 0
      # default
      assert item.active == false

      # Fifth row is the trailing empty row with defaults
      item = Enum.at(items, 4)
      # default
      assert item.name == "Unknown"
      # default
      assert item.age == 0
      # default
      assert item.active == false
    end
  end

  describe "parsing options" do
    test "trim_fields option" do
      # CSV with whitespace
      csv_string = """
      Item 1  ,  Description with spaces  ,  10.50
      Item 2,Description 2,20.75
      """

      # Test without trim
      items = TrimSchema.read_string(csv_string, trim_fields: false)
      assert length(items) == 2

      item = Enum.at(items, 0)
      # whitespace preserved
      assert item.item == "Item 1  "
      # whitespace preserved
      assert item.description == "  Description with spaces  "

      # Test with trim
      items = TrimSchema.read_string(csv_string)
      assert length(items) == 2

      item = Enum.at(items, 0)
      # whitespace trimmed
      assert item.item == "Item 1"
      # whitespace trimmed
      assert item.description == "Description with spaces"
    end

    test "delimiter option" do
      # CSV with pipe delimiter
      csv_string = """
      Item 1|Description 1|10.50
      Item 2|Description 2|20.75
      """

      # Test with pipe delimiter
      items = TrimSchema.read_string(csv_string, delimiter: "|")
      assert length(items) == 2

      item = Enum.at(items, 0)
      assert item.item == "Item 1"
      assert item.description == "Description 1"
      assert item.price == 10.50

      # CSV with tab delimiter
      csv_string = "Item 1\tDescription 1\t10.50\nItem 2\tDescription 2\t20.75"

      # Test with tab delimiter
      items = TrimSchema.read_string(csv_string, delimiter: "\t")
      assert length(items) == 2

      item = Enum.at(items, 0)
      assert item.item == "Item 1"
      assert item.description == "Description 1"
      assert item.price == 10.50
    end
  end

  describe "custom field conversions" do
    test "custom boolean values" do
      # CSV with custom boolean values
      csv_string = """
      Item 1,paid
      Item 2,billed
      Item 3,pending
      """

      items = CustomBoolean.read_string(csv_string)
      assert length(items) == 3

      # Verify custom boolean conversions
      # "paid" maps to true
      assert Enum.at(items, 0).paid == true
      # "billed" maps to false
      assert Enum.at(items, 1).paid == false
      # "pending" maps to false
      assert Enum.at(items, 2).paid == false
    end

    test "custom read/write functions" do
      # CSV with pipe-separated tags
      csv_string = """
      Product A,tag1|tag2|tag3
      Product B,red|blue
      """

      products = CustomConversion.read_string(csv_string)
      assert length(products) == 2

      # Verify tags were converted to lists
      product = Enum.at(products, 0)
      assert product.name == "Product A"
      assert is_list(product.tags)
      assert product.tags == ["tag1", "tag2", "tag3"]

      product = Enum.at(products, 1)
      assert product.name == "Product B"
      assert is_list(product.tags)
      assert product.tags == ["red", "blue"]
    end
  end

  describe "streaming operations" do
    test "streams data from string" do
      csv_string = Helpers.sample_csv_with_headers()

      # Stream first 2 records
      stream =
        csv_string
        |> FullSchema.read_string(headers: true, as_stream: true)
        |> Stream.take(2)
        |> Enum.to_list()

      assert length(stream) == 2
      assert Enum.at(stream, 0).first_name == "John"
      assert Enum.at(stream, 1).first_name == "Jane"
    end

    test "streams data from file" do
      # Create a temporary file with test data
      test_file = Helpers.create_temp_file(Helpers.sample_csv_with_headers())

      # Stream from file
      stream =
        test_file
        |> FullSchema.stream(headers: true)
        |> Enum.to_list()

      assert length(stream) == 3
      assert Enum.at(stream, 0).first_name == "John"
      assert Enum.at(stream, 1).first_name == "Jane"
      assert Enum.at(stream, 2).first_name == "Bob"
    end

    test "streams with transformation" do
      # Create a temporary file with test data
      test_file = Helpers.create_temp_file(Helpers.sample_csv_with_headers())

      # Stream with transformation
      result =
        test_file
        |> FullSchema.stream(headers: true)
        |> Stream.map(fn person -> Map.update!(person, :first_name, &String.upcase/1) end)
        |> Enum.to_list()

      assert length(result) == 3
      assert Enum.at(result, 0).first_name == "JOHN"
      assert Enum.at(result, 1).first_name == "JANE"
      assert Enum.at(result, 2).first_name == "BOB"
    end
  end
end
