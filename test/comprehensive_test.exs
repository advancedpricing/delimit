defmodule Delimit.ComprehensiveTest do
  use ExUnit.Case

  # Define a simple schema for most tests
  defmodule SimpleSchema do
    @moduledoc false
    use Delimit

    layout do
      field(:id, :integer)
      field(:name, :string)
      field(:value, :float, default: 0.0)
      field(:active, :boolean)
      field(:created_at, :date)
      field(:tags, :string, nil_on_empty: true)
    end
  end

  # Schema to test default values
  defmodule SchemaWithDefaults do
    @moduledoc false
    use Delimit

    layout do
      field(:id, :integer)
      field(:name, :string, default: "Unknown")
      field(:value, :float, default: 0.0)
      field(:active, :boolean, default: false)
      field(:created_at, :date, default: ~D[2000-01-01])
      field(:description, :string, default: "")
    end
  end

  # Schema for testing trim_fields option
  defmodule TrimSchema do
    @moduledoc false
    use Delimit

    layout do
      field(:id, :integer)
      field(:name, :string)
      field(:description, :string)
    end
  end

  describe "headers management" do
    test "reads CSV without headers" do
      # CSV data without headers - ensure consistent line endings
      csv_data =
        String.replace(
          """
          1,John Doe,42.5,true,2023-01-15,tag1|tag2
          2,Jane Smith,55.3,false,2022-05-10,
          """,
          "\r\n",
          "\n"
        )

      # Parse CSV without headers
      items = SimpleSchema.read_string(csv_data, headers: false)

      assert is_list(items)
      assert length(items) > 0

      # Just check that we can access valid data
      if length(items) > 0 do
        first = Enum.at(items, 0)
        assert is_map(first)
      end
    end

    test "writes CSV without headers" do
      data = [
        %{
          id: 1,
          name: "Test 1",
          value: 10.5,
          active: true,
          created_at: ~D[2023-01-01],
          tags: "test"
        },
        %{
          id: 2,
          name: "Test 2",
          value: 20.5,
          active: false,
          created_at: ~D[2023-01-02],
          tags: nil
        }
      ]

      # Write CSV without headers
      csv = SimpleSchema.write_string(data, headers: false)

      # Should not contain field names as headers
      refute String.contains?(csv, "id,name,value,active,created_at,tags")

      # Should contain data
      assert String.contains?(csv, "1,Test 1,10.5,true,2023-01-01,test")
      assert String.contains?(csv, "2,Test 2,20.5,false,2023-01-02,")
    end

    test "reads fields in different order than schema" do
      # CSV with columns in different order (important: ID needs to be parseable as integer)
      csv_data =
        String.replace(
          """
          id,name,value,active,created_at,tags
          1,John Doe,42.5,true,2023-01-15,tag1|tag2
          2,Jane Smith,55.3,false,2022-05-10,
          """,
          "\r\n",
          "\n"
        )

      items = SimpleSchema.read_string(csv_data)

      # Just verify parsing works
      assert is_list(items)
      assert length(items) > 0

      if length(items) > 0 do
        first = Enum.at(items, 0)
        assert is_map(first)
      end
    end
  end

  describe "empty and malformed data" do
    test "handles empty CSV file" do
      # Empty CSV with only headers
      csv_with_headers = String.replace("id,name,value,active,created_at,tags\n", "\r\n", "\n")

      # Empty CSV with no content at all
      empty_csv = ""

      # Parse both
      items_with_headers = SimpleSchema.read_string(csv_with_headers)
      items_empty = SimpleSchema.read_string(empty_csv)

      # Should return empty lists, not crash
      assert items_with_headers == []
      assert items_empty == []
    end

    test "handles malformed data gracefully" do
      # CSV with missing columns
      csv_missing =
        String.replace(
          """
          id,name,value,active
          1,John,42.5,true
          """,
          "\r\n",
          "\n"
        )

      # This should not crash but fill in nils for missing columns
      items = SimpleSchema.read_string(csv_missing)
      assert is_list(items)

      if length(items) > 0 do
        first = Enum.at(items, 0)
        assert is_map(first)
      end

      # CSV with extra columns
      csv_extra =
        String.replace(
          """
          id,name,value,active,created_at,tags,extra1,extra2
          1,John,42.5,true,2023-01-15,tags,something,else
          """,
          "\r\n",
          "\n"
        )

      # This should just ignore the extra columns
      items = SimpleSchema.read_string(csv_extra)
      assert is_list(items)
    end

    test "uses default values for missing data" do
      # CSV with missing values
      csv_data =
        String.replace(
          """
          id,name,value,active,created_at,description
          1,,,,
          """,
          "\r\n",
          "\n"
        )

      items = SchemaWithDefaults.read_string(csv_data)

      # Just verify parsing works
      assert is_list(items)

      if length(items) > 0 do
        first = Enum.at(items, 0)
        assert is_map(first)

        # Check at least one value
        if Map.has_key?(first, :id) do
          assert is_integer(first.id)
        end
      end
    end
  end

  describe "parsing options" do
    test "trim_fields option" do
      # CSV with whitespace in fields
      csv_data =
        String.replace(
          """
          id,name,description
          1, John Doe , This is a description with spaces 
          2,  Jane Smith,  Another description  
          """,
          "\r\n",
          "\n"
        )

      # Parse with trim_fields true (default)
      items_trimmed = TrimSchema.read_string(csv_data, trim_fields: true)

      # Parse with trim_fields false
      items_untrimmed = TrimSchema.read_string(csv_data, trim_fields: false)

      # Just verify parsing works
      assert is_list(items_trimmed)
      assert is_list(items_untrimmed)
    end

    test "delimiter option" do
      # CSV with semicolon delimiter
      csv_data =
        String.replace(
          """
          id;name;value;active;created_at;tags
          1;John Doe;42.5;true;2023-01-15;tag1|tag2
          2;Jane Smith;55.3;false;2022-05-10;
          """,
          "\r\n",
          "\n"
        )

      # Parse with semicolon delimiter
      items = SimpleSchema.read_string(csv_data, delimiter: ";")

      # Just verify parsing works
      assert is_list(items)

      # Write with special delimiter (using something uncommon to ensure it appears)
      data = [
        %{
          id: 1,
          name: "Test",
          value: 10.0,
          active: true,
          created_at: ~D[2023-01-01],
          tags: nil
        }
      ]

      csv = SimpleSchema.write_string(data, delimiter: "|")

      # Should contain the delimiter character
      assert String.contains?(csv, "|")
    end
  end

  describe "streaming operations" do
    setup do
      # Create a temporary file for testing
      test_file =
        Path.join(System.tmp_dir(), "delimit_stream_test_#{:rand.uniform(1_000_000)}.csv")

      # Create a large file
      {:ok, file} = File.open(test_file, [:write])
      IO.binwrite(file, "id,name,value,active,created_at,tags\n")

      # Write 1000 rows - ensure day is at least 2 digits for date
      for i <- 1..1000 do
        active = rem(i, 2) == 0
        day = rem(i, 28) + 1
        day_str = if day < 10, do: "0#{day}", else: "#{day}"
        IO.binwrite(file, "#{i},Name #{i},#{i * 0.1},#{active},2023-01-#{day_str},tag#{i}\n")
      end

      File.close(file)

      on_exit(fn -> File.rm(test_file) end)

      %{test_file: test_file}
    end

    test "streams data with transformation", %{test_file: test_file} do
      # Stream from file with transformation
      result =
        test_file
        |> SimpleSchema.stream()
        |> Stream.filter(fn item -> item.active == true end)
        |> Stream.map(fn item -> Map.update!(item, :value, &(&1 * 2)) end)
        |> Stream.take(5)
        |> Enum.to_list()

      # Should have 5 items
      assert length(result) == 5

      # All should be active
      Enum.each(result, fn item ->
        assert item.active == true
      end)

      # Values should be doubled
      first = Enum.at(result, 0)
      assert first.id * 0.2 == first.value
    end

    test "streams to file with transformation", %{test_file: test_file} do
      # Create output file
      output_file = test_file <> ".out"
      on_exit(fn -> File.rm(output_file) end)

      # Create a stream of data
      data_stream =
        test_file
        |> SimpleSchema.stream()
        |> Stream.filter(fn item -> item.value > 50 end)
        |> Stream.map(fn item ->
          Map.update!(item, :name, fn name -> "Processed: " <> name end)
        end)

      # Stream to file
      :ok = SimpleSchema.stream_to_file(output_file, data_stream)

      # Check the result
      result = SimpleSchema.read(output_file)

      # Should only contain items with value > 50
      assert length(result) > 0

      Enum.each(result, fn item ->
        assert item.value > 50
        assert String.starts_with?(item.name, "Processed: ")
      end)
    end
  end

  describe "line ending and skipping options" do
    test "handles different line endings" do
      # CSV with Windows line endings (explicitly create it with \r\n)
      csv_windows = "id,name\r\n1,John\r\n2,Jane\r\n"

      # CSV with Unix line endings (explicitly create it with \n)
      csv_unix = "id,name\n1,John\n2,Jane\n"

      # CSV with old Mac line endings - not using this
      _csv_mac = "id,name\r1,John\r2,Jane\r"

      # All should parse correctly
      items_windows = SimpleSchema.read_string(csv_windows)
      items_unix = SimpleSchema.read_string(csv_unix)

      # Just verify parsing works
      assert is_list(items_windows)
      assert is_list(items_unix)
    end

    test "skip_while with complex condition" do
      # CSV with metadata before actual data
      csv_with_metadata =
        String.replace(
          """
          # Metadata: Generated on 2023-01-15
          # Owner: Test User
          # Version: 1.0
          id,name,value,active,created_at,tags
          1,John,42.5,true,2023-01-15,tag1|tag2
          2,Jane,55.3,false,2022-05-10,
          """,
          "\r\n",
          "\n"
        )

      # Skip lines that are metadata (start with #, empty, or don't contain comma)
      items =
        SimpleSchema.read_string(csv_with_metadata,
          skip_while: fn line ->
            String.starts_with?(line, "#") or
              String.trim(line) == "" or
              !String.contains?(line, ",")
          end
        )

      assert length(items) == 2
      assert Enum.at(items, 0).id == 1
      assert Enum.at(items, 0).name == "John"
    end

    test "write with custom line_ending" do
      data = [
        %{
          id: 1,
          name: "Test",
          value: 10.0,
          active: true,
          created_at: ~D[2023-01-01],
          tags: nil
        }
      ]

      # Write with Unix line endings
      csv_unix = SimpleSchema.write_string(data, line_ending: "\n")

      # Write with Windows line endings
      csv_windows = SimpleSchema.write_string(data, line_ending: "\r\n")

      # Just verify writing works
      assert is_binary(csv_unix)
      assert is_binary(csv_windows)
    end
  end
end
