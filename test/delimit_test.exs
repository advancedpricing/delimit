defmodule DelimitTest do
  use ExUnit.Case, async: true

  alias Delimit.TestSupport.Schemas.SimpleSchema

  doctest Delimit

  describe "core module functionality" do
    test "creates schema correctly using DSL" do
      schema = SimpleSchema.__delimit_schema__()

      assert schema.module == SimpleSchema
      assert length(schema.fields) == 3

      field_names = Enum.map(schema.fields, fn field -> field.name end)
      assert field_names == [:name, :age, :active]

      age_field = Enum.find(schema.fields, fn field -> field.name == :age end)
      assert age_field.type == :integer
      assert age_field.opts == []
    end

    test "generates typespec and struct correctly" do
      # Verify the struct exists and contains expected fields
      person = %SimpleSchema{name: "John", age: 30, active: true}
      assert person.__struct__ == SimpleSchema
      assert person.name == "John"
      assert person.age == 30
      assert person.active == true
    end

    test "can read and write data" do
      # Create properly formatted CSV string
      csv_string = "John,30,true\nJane,28,false"

      # Read the data using the schema
      people = SimpleSchema.read_string(csv_string)
      assert length(people) == 2

      # Verify field types are correctly converted
      first_person = Enum.at(people, 0)
      assert is_binary(first_person.name)
      assert is_integer(first_person.age)
      assert is_boolean(first_person.active)

      # Write the data back to a string
      output = SimpleSchema.write_string(people)
      assert is_binary(output)
      assert String.contains?(output, "Jane,28,false")
    end

    test "provides access to schema definition" do
      schema = SimpleSchema.__delimit_schema__()
      assert is_map(schema)
      assert schema.module == SimpleSchema
      assert is_list(schema.fields)
    end
  end
end
