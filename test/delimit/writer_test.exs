defmodule Delimit.WriterTest do
  use ExUnit.Case, async: true

  alias Delimit.TestSupport.Helpers
  alias Delimit.TestSupport.Schemas.CustomBoolean
  alias Delimit.TestSupport.Schemas.CustomConversion
  alias Delimit.TestSupport.Schemas.FullSchema
  alias Delimit.TestSupport.Schemas.SimpleSchema

  describe "basic writing functionality" do
    test "writes CSV without headers" do
      # Create test data
      people = [
        %FullSchema{
          first_name: "John",
          last_name: "Doe",
          age: 30,
          salary: 50_000.5,
          hired_date: ~D[2020-01-15],
          active: true,
          notes: "Good employee"
        },
        %FullSchema{
          first_name: "Jane",
          last_name: "Smith",
          age: 28,
          salary: 55_000.75,
          hired_date: ~D[2019-05-20],
          active: true,
          notes: nil
        }
      ]

      # Write to string without headers
      output = FullSchema.write_string(people, headers: false)

      # Verify output structure
      assert is_binary(output)
      # Basic structure checks
      assert String.contains?(output, "John,Doe,30,50000.5,2020-01-15,true,Good employee")
      assert String.contains?(output, "Jane,Smith,28,55000.75,2019-05-20,true,")

      # No headers should appear
      refute String.contains?(output, "first_name,last_name")
    end

    test "writes CSV with headers" do
      # Create test data
      people = [
        %FullSchema{
          first_name: "John",
          last_name: "Doe",
          age: 30,
          salary: 50_000.5,
          hired_date: ~D[2020-01-15],
          active: true,
          notes: "Good employee"
        }
      ]

      # Write to string with headers
      output = FullSchema.write_string(people, headers: true)

      # Verify output structure
      assert is_binary(output)

      # Headers should appear
      assert String.contains?(
               output,
               "first_name,last_name,age,salary,hired_date,active,notes"
             )

      # Data should appear
      assert String.contains?(output, "John,Doe,30,50000.5,2020-01-15,true,Good employee")
    end

    test "write to file" do
      # Create test data
      people = [
        %SimpleSchema{
          name: "John Doe",
          age: 30,
          active: true
        },
        %SimpleSchema{
          name: "Jane Smith",
          age: 28,
          active: false
        }
      ]

      # Create a temporary file for testing
      test_file = Helpers.temp_file_path()

      # Register cleanup
      on_exit(fn -> File.rm(test_file) end)

      # Write to file
      :ok = SimpleSchema.write(test_file, people)

      # Read the file to verify
      {:ok, content} = File.read(test_file)

      # Verify the content
      assert String.contains?(content, "John Doe,30,true")
      assert String.contains?(content, "Jane Smith,28,false")
    end
  end

  describe "writing options" do
    test "delimiter option" do
      items = [
        %SimpleSchema{name: "Item 1", age: 10, active: true},
        %SimpleSchema{name: "Item 2", age: 20, active: false}
      ]

      # Test with pipe delimiter
      output = SimpleSchema.write_string(items, delimiter: "|")

      # Verify pipe delimiter was used
      assert String.contains?(output, "Item 1|10|true")
      assert String.contains?(output, "Item 2|20|false")

      # Test with tab delimiter
      output = SimpleSchema.write_string(items, delimiter: "\t")

      # Verify tab delimiter was used
      assert String.contains?(output, "Item 1\t10\ttrue")
      assert String.contains?(output, "Item 2\t20\tfalse")
    end

    test "line_ending option" do
      items = [
        %SimpleSchema{name: "Item 1", age: 10, active: true},
        %SimpleSchema{name: "Item 2", age: 20, active: false}
      ]

      # Test with Windows line ending
      output = SimpleSchema.write_string(items, line_ending: "\r\n")

      # Verify Windows line ending was used
      assert String.contains?(output, "Item 1,10,true\r\n")

      # Test with Unix line ending
      output = SimpleSchema.write_string(items, line_ending: "\n")

      # Verify Unix line ending was used
      assert String.contains?(output, "Item 1,10,true\n")
    end

    test "escape character option" do
      # Items with commas in the name
      items = [
        %SimpleSchema{name: "Item, with comma", age: 10, active: true},
        %SimpleSchema{name: "Item \"quoted\"", age: 20, active: false}
      ]

      # Default escape character
      output = SimpleSchema.write_string(items)

      # Verify default escaping
      assert String.contains?(output, "\"Item, with comma\",10,true")
      assert String.contains?(output, ~s("Item ""quoted""",20,false))

      # Custom escape character
      output = SimpleSchema.write_string(items, escape: "'")

      # Verify custom escaping
      assert String.contains?(output, "'Item, with comma',10,true")
    end
  end

  describe "custom field conversions" do
    test "custom boolean values" do
      # Items with custom boolean values
      items = [
        %CustomBoolean{item: "Item 1", paid: true},
        %CustomBoolean{item: "Item 2", paid: false}
      ]

      # Write with custom boolean values
      output = CustomBoolean.write_string(items)

      # Verify custom boolean values
      assert String.contains?(output, "Item 1,paid")
      assert String.contains?(output, "Item 2,billed")
    end

    test "custom write functions" do
      # Products with tag lists
      products = [
        %CustomConversion{name: "Product A", tags: ["tag1", "tag2", "tag3"]},
        %CustomConversion{name: "Product B", tags: ["red", "blue"]}
      ]

      # Write with custom conversion
      output = CustomConversion.write_string(products)

      # Verify tags were converted to pipe-separated strings
      assert String.contains?(output, "Product A,tag1|tag2|tag3")
      assert String.contains?(output, "Product B,red|blue")
    end
  end

  describe "streaming operations" do
    test "streams to file" do
      # Create a stream of data
      stream =
        Stream.map(1..5, fn i ->
          %SimpleSchema{
            name: "Person #{i}",
            age: 20 + i,
            active: rem(i, 2) == 0
          }
        end)

      # Create a temporary file for testing
      test_file = Helpers.temp_file_path()

      # Register cleanup
      on_exit(fn -> File.rm(test_file) end)

      # Stream to file
      :ok = SimpleSchema.stream_to_file(test_file, stream)

      # Read the file to verify
      {:ok, content} = File.read(test_file)

      # Verify the content contains all 5 records
      assert String.contains?(content, "Person 1,21,false")
      assert String.contains?(content, "Person 2,22,true")
      assert String.contains?(content, "Person 3,23,false")
      assert String.contains?(content, "Person 4,24,true")
      assert String.contains?(content, "Person 5,25,false")
    end
  end

  describe "round-trip operations" do
    test "write then read preserves data" do
      # Original data
      original_people = [
        %FullSchema{
          first_name: "John",
          last_name: "Doe",
          age: 30,
          salary: 50_000.5,
          hired_date: ~D[2020-01-15],
          active: true,
          notes: "Good employee"
        },
        %FullSchema{
          first_name: "Jane",
          last_name: "Smith",
          age: 28,
          salary: 55_000.75,
          hired_date: ~D[2019-05-20],
          active: true,
          notes: nil
        }
      ]

      # Write to string
      output = FullSchema.write_string(original_people)

      # Read back
      read_people = FullSchema.read_string(output)

      # Verify data is preserved
      assert length(read_people) == 2

      first_person = Enum.at(read_people, 0)
      assert first_person.first_name == "John"
      assert first_person.last_name == "Doe"
      assert first_person.age == 30
      assert first_person.salary == 50_000.5
      assert first_person.hired_date == ~D[2020-01-15]
      assert first_person.active == true
      assert first_person.notes == "Good employee"

      second_person = Enum.at(read_people, 1)
      assert second_person.first_name == "Jane"
      assert second_person.last_name == "Smith"
      assert second_person.age == 28
      assert second_person.salary == 55_000.75
      assert second_person.hired_date == ~D[2019-05-20]
      assert second_person.active == true
      assert second_person.notes == nil
    end
  end
end
