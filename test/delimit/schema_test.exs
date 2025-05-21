defmodule Delimit.SchemaTest do
  use ExUnit.Case, async: true

  alias Delimit.Schema
  alias Delimit.TestSupport.Schemas.Address
  alias Delimit.TestSupport.Schemas.Customer
  alias Delimit.TestSupport.Schemas.FullSchema
  alias Delimit.TestSupport.Schemas.SimpleSchema

  describe "schema creation and management" do
    test "creates a new schema" do
      schema = Schema.new(SimpleSchema)
      assert schema.module == SimpleSchema
      assert schema.fields == []
      assert schema.embeds == %{}
      assert schema.options == []
    end

    test "creates a new schema with options" do
      schema = Schema.new(SimpleSchema, headers: true, delimiter: ",")
      assert schema.options == [headers: true, delimiter: ","]
    end

    test "adds a field to schema" do
      schema = Schema.new(SimpleSchema)
      schema = Schema.add_field(schema, :name, :string)

      assert length(schema.fields) == 1
      field = hd(schema.fields)
      assert field.name == :name
      assert field.type == :string
      assert field.opts == []
    end

    test "adds a field with options" do
      schema = Schema.new(SimpleSchema)

      schema =
        Schema.add_field(schema, :active, :boolean, true_values: ["YES"], false_values: ["NO"])

      field = hd(schema.fields)
      assert field.name == :active
      assert field.type == :boolean
      assert field.opts == [true_values: ["YES"], false_values: ["NO"]]
    end

    test "gets field names" do
      schema = Schema.new(SimpleSchema)
      schema = Schema.add_field(schema, :first_name, :string)
      schema = Schema.add_field(schema, :last_name, :string)
      schema = Schema.add_field(schema, :age, :integer)

      assert Schema.field_names(schema) == [:first_name, :last_name, :age]
    end

    test "gets a field by name" do
      schema = Schema.new(SimpleSchema)
      schema = Schema.add_field(schema, :first_name, :string)
      schema = Schema.add_field(schema, :last_name, :string)

      field = Schema.get_field(schema, :first_name)
      assert field.name == :first_name
      assert field.type == :string

      field = Schema.get_field(schema, :missing)
      assert is_nil(field)
    end
  end

  describe "embedded schemas" do
    test "adds an embedded schema" do
      schema = Schema.new(Customer)
      schema = Schema.add_embed(schema, :address, Address)

      assert length(schema.fields) == 1
      field = hd(schema.fields)
      assert field.name == :address
      assert field.type == :embed

      assert Map.has_key?(schema.embeds, :address)
      assert schema.embeds.address == Address
    end

    test "adds an embedded schema with options" do
      schema = Schema.new(Customer)
      schema = Schema.add_embed(schema, :address, Address, prefix: "addr_")

      field = hd(schema.fields)
      assert field.name == :address
      assert field.type == :embed
      assert field.opts == [prefix: "addr_"]
    end

    test "generates headers for embedded schemas" do
      # Get schema from the Customer module
      schema = Customer.__delimit_schema__()

      headers = Schema.headers(schema)

      # Check for main fields
      assert "name" in headers
      assert "contact_email" in headers

      # Check for embedded fields with prefixes
      assert "billing_address_street" in headers
      assert "billing_address_city" in headers
      assert "shipping_address_street" in headers
      assert "shipping_address_city" in headers
    end
  end

  describe "data conversion" do
    test "converts row to struct" do
      # Use the schema from our defined FullSchema module
      schema = FullSchema.__delimit_schema__()

      struct =
        Schema.to_struct(schema, [
          "John",
          "Doe",
          "42",
          "50000.5",
          "2020-01-15",
          "true",
          "Notes"
        ])

      assert struct.first_name == "John"
      assert struct.last_name == "Doe"
      assert struct.age == 42
      assert struct.salary == 50_000.5
      assert struct.hired_date == ~D[2020-01-15]
      assert struct.active == true
      assert struct.notes == "Notes"
    end

    test "converts struct to row" do
      schema = FullSchema.__delimit_schema__()

      # Create a proper struct
      person = %FullSchema{
        first_name: "John",
        last_name: "Doe",
        age: 42,
        salary: 50_000.5,
        hired_date: ~D[2020-01-15],
        active: true,
        notes: "Notes"
      }

      row = Schema.to_row(schema, person)

      # The row will contain values for all fields in the schema
      # first_name
      assert Enum.at(row, 0) == "John"
      # last_name
      assert Enum.at(row, 1) == "Doe"
      # age
      assert Enum.at(row, 2) == "42"
      # salary
      assert Enum.at(row, 3) == "50000.5"
      # hired_date
      assert Enum.at(row, 4) == "2020-01-15"
      # active
      assert Enum.at(row, 5) == "true"
      # notes
      assert Enum.at(row, 6) == "Notes"
    end

    test "handles nil values" do
      schema = FullSchema.__delimit_schema__()

      # Create a struct with nil values
      person = %FullSchema{
        first_name: "John",
        last_name: nil,
        age: nil,
        salary: nil,
        hired_date: nil,
        active: nil,
        notes: nil
      }

      row = Schema.to_row(schema, person)

      # first_name
      assert Enum.at(row, 0) == "John"
      # last_name (nil)
      assert Enum.at(row, 1) == ""
      # age (nil)
      assert Enum.at(row, 2) == ""
      # salary (nil)
      assert Enum.at(row, 3) == ""
      # hired_date (nil)
      assert Enum.at(row, 4) == ""
      # active (nil)
      assert Enum.at(row, 5) == ""
      # notes (nil)
      assert Enum.at(row, 6) == ""
    end

    test "generates headers from schema" do
      schema = FullSchema.__delimit_schema__()

      headers = Schema.headers(schema)
      assert "first_name" in headers
      assert "last_name" in headers
      assert "age" in headers
      assert "salary" in headers
      assert "hired_date" in headers
      assert "active" in headers
      assert "notes" in headers
    end
  end

  describe "type conversion" do
    test "converts type to typespec" do
      assert Schema.type_to_typespec(:string) == quote(do: String.t())
      assert Schema.type_to_typespec(:integer) == quote(do: integer())
      assert Schema.type_to_typespec(:float) == quote(do: float())
      assert Schema.type_to_typespec(:boolean) == quote(do: boolean())
      assert Schema.type_to_typespec(:date) == quote(do: Date.t())
      assert Schema.type_to_typespec(:time) == quote(do: Time.t())
      assert Schema.type_to_typespec(:datetime) == quote(do: DateTime.t())
      assert Schema.type_to_typespec(:naive_datetime) == quote(do: NaiveDateTime.t())

      # List type
      assert Schema.type_to_typespec({:list, :string}) == quote(do: list(String.t()))

      # Unknown type defaults to any
      assert Schema.type_to_typespec(:unknown) == quote(do: any())
    end
  end
end
