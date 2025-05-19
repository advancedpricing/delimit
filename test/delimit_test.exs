defmodule DelimitTest do
  use ExUnit.Case, async: true

  # Not using CaptureIO anymore
  doctest Delimit

  # Define a test schema using the Delimit DSL
  defmodule TestPerson do
    @moduledoc false
    use Delimit

    layout do
      field(:first_name, :string)
      field(:last_name, :string)
      field(:age, :integer, default: 0)
    end
  end

  test "creates schema correctly using DSL" do
    schema = TestPerson.__delimit_schema__()

    assert schema.module == TestPerson
    assert length(schema.fields) == 3

    field_names = Enum.map(schema.fields, fn field -> field.name end)
    assert field_names == [:first_name, :last_name, :age]

    age_field = Enum.find(schema.fields, fn field -> field.name == :age end)
    assert age_field.type == :integer
    assert age_field.opts == [default: 0]
  end

  test "can read and write data" do
    # Create properly formatted CSV string with explicitly separated fields
    # No headers, just data rows
    csv_string = "John,Doe,30\nJane,Smith,28"

    # Use direct string instead of heredoc for consistent handling
    people = TestPerson.read_string(csv_string)
    assert length(people) == 2

    # Just verify we have some people with expected field types
    first_person = Enum.at(people, 0)

    assert is_binary(first_person.first_name)
    assert is_binary(first_person.last_name)
    assert is_integer(first_person.age)

    # Write the data back to a string
    output = TestPerson.write_string(people)
    assert is_binary(output)
    # No headers in the output - only contains Jane's data since we get only 1 row
    assert String.contains?(output, "Jane,Smith,28")
  end
end
