defmodule Delimit.SchemaTest do
  use ExUnit.Case, async: true

  alias Delimit.Schema

  # Define a proper test module with Delimit
  defmodule TestPerson do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:first_name, :string)
      field(:last_name, :string)
      field(:age, :integer)
      field(:active, :boolean, true_values: ["YES"], false_values: ["NO"])
    end
  end

  defmodule TestAddress do
    @moduledoc false
    use Delimit

    layout do
      field(:street, :string)
      field(:city, :string)
      field(:zip, :string)
    end
  end

  describe "schema management" do
    test "creates a new schema" do
      schema = Schema.new(TestPerson)
      assert schema.module == TestPerson
      assert schema.fields == []
      assert schema.embeds == %{}
      assert schema.options == []
    end

    test "creates a new schema with options" do
      schema = Schema.new(TestPerson, headers: true, delimiter: ",")
      assert schema.options == [headers: true, delimiter: ","]
    end

    test "adds a field to schema" do
      schema = Schema.new(TestPerson)
      schema = Schema.add_field(schema, :name, :string)

      assert length(schema.fields) == 1
      field = hd(schema.fields)
      assert field.name == :name
      assert field.type == :string
      assert field.opts == []
    end

    test "adds a field with options" do
      schema = Schema.new(TestPerson)

      schema =
        Schema.add_field(schema, :active, :boolean, true_values: ["YES"], false_values: ["NO"])

      field = hd(schema.fields)
      assert field.name == :active
      assert field.type == :boolean
      assert field.opts == [true_values: ["YES"], false_values: ["NO"]]
    end

    test "adds an embedded schema" do
      schema = Schema.new(TestPerson)
      schema = Schema.add_embed(schema, :address, TestAddress)

      assert length(schema.fields) == 1
      field = hd(schema.fields)
      assert field.name == :address
      assert field.type == :embed

      assert Map.has_key?(schema.embeds, :address)
      assert schema.embeds.address == TestAddress
    end

    test "gets field names" do
      schema = Schema.new(TestPerson)
      schema = Schema.add_field(schema, :first_name, :string)
      schema = Schema.add_field(schema, :last_name, :string)
      schema = Schema.add_field(schema, :age, :integer)

      assert Schema.field_names(schema) == [:first_name, :last_name, :age]
    end

    test "gets a field by name" do
      schema = Schema.new(TestPerson)
      schema = Schema.add_field(schema, :first_name, :string)
      schema = Schema.add_field(schema, :last_name, :string)

      field = Schema.get_field(schema, :first_name)
      assert field.name == :first_name
      assert field.type == :string

      field = Schema.get_field(schema, :missing)
      assert is_nil(field)
    end
  end

  describe "data conversion" do
    test "converts row to struct" do
      # Use the schema from our defined TestPerson module
      schema = TestPerson.__delimit_schema__()

      struct = Schema.to_struct(schema, ["John Doe", "", "", "42", ""])
      assert struct.name == "John Doe"
      assert struct.age == 42
    end

    test "converts row to struct with headers" do
      schema = TestPerson.__delimit_schema__()

      struct = Schema.to_struct(schema, ["42", "John Doe"], ["age", "name"])
      assert struct.name == "John Doe"
      assert struct.age == 42
    end

    test "converts struct to row" do
      schema = TestPerson.__delimit_schema__()

      # Create a proper struct
      person = %TestPerson{name: "John Doe", age: 42}
      row = Schema.to_row(schema, person)

      # The row will contain values for all fields in the schema
      # name
      assert Enum.at(row, 0) == "John Doe"
      # age
      assert Enum.at(row, 3) == "42"
    end

    test "generates headers from schema" do
      schema = TestPerson.__delimit_schema__()

      headers = Schema.headers(schema)
      assert "name" in headers
      assert "first_name" in headers
      assert "last_name" in headers
    end
  end
end
