defmodule Delimit.SchemaTest do
  use ExUnit.Case

  alias Delimit.Schema

  describe "schema management" do
    test "creates a new schema" do
      schema = Schema.new(TestModule)
      assert schema.module == TestModule
      assert schema.fields == []
      assert schema.embeds == %{}
      assert schema.options == []
    end

    test "creates a new schema with options" do
      schema = Schema.new(TestModule, headers: true, delimiter: ",")
      assert schema.options == [headers: true, delimiter: ","]
    end

    test "adds a field to schema" do
      schema = Schema.new(TestModule)
      schema = Schema.add_field(schema, :name, :string)
      
      assert length(schema.fields) == 1
      field = hd(schema.fields)
      assert field.name == :name
      assert field.type == :string
      assert field.opts == []
    end

    test "adds a field with options" do
      schema = Schema.new(TestModule)
      schema = Schema.add_field(schema, :active, :boolean, [true_values: ["YES"], false_values: ["NO"]])
      
      field = hd(schema.fields)
      assert field.name == :active
      assert field.type == :boolean
      assert field.opts == [true_values: ["YES"], false_values: ["NO"]]
    end

    test "adds an embedded schema" do
      schema = Schema.new(TestModule)
      schema = Schema.add_embed(schema, :address, AddressModule)
      
      assert length(schema.fields) == 1
      field = hd(schema.fields)
      assert field.name == :address
      assert field.type == :embed
      
      assert Map.has_key?(schema.embeds, :address)
      assert schema.embeds.address == AddressModule
    end

    test "gets field names" do
      schema = Schema.new(TestModule)
      schema = Schema.add_field(schema, :first_name, :string)
      schema = Schema.add_field(schema, :last_name, :string)
      schema = Schema.add_field(schema, :age, :integer)
      
      assert Schema.field_names(schema) == [:first_name, :last_name, :age]
    end

    test "gets a field by name" do
      schema = Schema.new(TestModule)
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
      schema = Schema.new(TestModule)
      schema = Schema.add_field(schema, :name, :string)
      schema = Schema.add_field(schema, :age, :integer)
      
      struct = Schema.to_struct(schema, ["John Doe", "42"])
      assert struct.name == "John Doe"
      assert struct.age == 42
    end

    test "converts row to struct with headers" do
      schema = Schema.new(TestModule)
      schema = Schema.add_field(schema, :name, :string)
      schema = Schema.add_field(schema, :age, :integer)
      
      struct = Schema.to_struct(schema, ["42", "John Doe"], ["age", "name"])
      assert struct.name == "John Doe"
      assert struct.age == 42
    end

    test "converts struct to row" do
      schema = Schema.new(TestModule)
      schema = Schema.add_field(schema, :name, :string)
      schema = Schema.add_field(schema, :age, :integer)
      
      row = Schema.to_row(schema, %{name: "John Doe", age: 42})
      assert row == ["John Doe", "42"]
    end

    test "generates headers from schema" do
      schema = Schema.new(TestModule)
      schema = Schema.add_field(schema, :first_name, :string)
      schema = Schema.add_field(schema, :last_name, :string)
      
      assert Schema.headers(schema) == ["first_name", "last_name"]
    end
  end
end