defmodule DelimitTest do
  use ExUnit.Case

  doctest Delimit

  # Define a test schema using the Delimit DSL
  defmodule TestPerson do
    use Delimit

    layout do
      field :first_name, :string
      field :last_name, :string
      field :age, :integer, default: 0
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
    csv = "first_name,last_name,age\nJohn,Doe,30\nJane,Smith,28"
    
    people = TestPerson.read_string(csv)
    assert length(people) > 0
    
    # Just verify we have some people with expected field types
    first_person = Enum.at(people, 0)
    assert is_binary(first_person.first_name)
    assert is_binary(first_person.last_name)
    assert is_integer(first_person.age)
    
    # Write the data back to a string
    output = TestPerson.write_string(people)
    assert is_binary(output)
    assert String.contains?(output, "first_name")
    assert String.contains?(output, "last_name")
  end
end
