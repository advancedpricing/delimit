# Delimit

[![Hex.pm Version](https://img.shields.io/hexpm/v/delimit.svg)](https://hex.pm/packages/delimit)
[![Hex.pm License](https://img.shields.io/hexpm/l/delimit.svg)](https://github.com/jcowgar/delimit/blob/main/LICENSE)

Delimit is a powerful yet elegant library for reading and writing delimited data files (CSV, TSV, PSV, SSV) in Elixir. Inspired by Ecto, it allows you to define schemas for your delimited data, providing strong typing with structs, validation, and transformation capabilities. By defining the structure of your data, Delimit enables type-safe parsing and generation with minimal boilerplate code.

## Features

- **Schema-based approach**: Define the structure of your delimited files using Ecto-like schemas
- **Strong typing with structs**: Convert between string values and proper Elixir types in type-safe structs
- **Full TypeSpecs**: Automatically generated type specifications for your schemas
- **Streaming support**: Process large files efficiently with Elixir streams
- **Customizable parsing**: Configure delimiters, headers, type conversion, and more
- **Embedded schemas**: Nest schemas for complex data structures
- **Custom transformations**: Add your own read/write functions for special data formats
- **Memory efficient**: Stream large files without loading everything into memory

## Installation

Add `delimit` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:delimit, "~> 0.1.0"}
  ]
end
```

Then fetch your dependencies:

```bash
mix deps.get
```

## Quick Start

### Define a schema

Define a schema that represents the structure of your delimited file:

```elixir
defmodule MyApp.Person do
  use Delimit

  layout do
    field :first_name, :string
    field :last_name, :string
    field :age, :integer
    field :salary, :float
    field :birthday, :date, format: "YYYY-MM-DD"
    field :active, :boolean
    field :notes, :string, nil_on_empty: true
  end
end
```

This automatically creates a struct with type specifications:

```elixir
@type t :: %__MODULE__{
  first_name: String.t(),
  last_name: String.t(),
  age: integer(),
  salary: float(),
  birthday: Date.t(),
  active: boolean(),
  notes: String.t()
}
```

### Reading data

Read data from a file:

```elixir
# Read all records at once - returns a list of structs
people = MyApp.Person.read("people.csv")
first_person = List.first(people) # returns a %MyApp.Person{} struct

# Stream records for better memory efficiency
people_stream =
  "large_file.csv"
  |> MyApp.Person.stream()
  |> Stream.filter(fn person -> person.age > 30 end)
  |> Stream.map(fn person -> %{person | salary: person.salary * 1.1} end)
  |> Enum.to_list()

# Read from a string
csv_data = "first_name,last_name,age\nJohn,Doe,42"
people = MyApp.Person.read_string(csv_data)
```

### Writing data

Write data to a file:

```elixir
people = [
  %MyApp.Person{first_name: "John", last_name: "Doe", age: 42,
    salary: 50000.0, birthday: ~D[1980-01-15], active: true, notes: "Senior developer"},
  %MyApp.Person{first_name: "Jane", last_name: "Smith", age: 35,
    salary: 60000.0, birthday: ~D[1987-05-22], active: true, notes: nil}
]

# Write all records at once
:ok = MyApp.Person.write("people.csv", people)

# Write to a string
csv_string = MyApp.Person.write_string(people)

# Stream data to a file (memory efficient)
stream = Stream.map(1..1000, fn i ->
  %MyApp.Person{
    first_name: "User#{i}",
    last_name: "Test",
    age: 20 + rem(i, 50),
    salary: 30_000.0 + (i * 100),
    birthday: Date.add(~D[2000-01-01], i),
    active: rem(i, 2) == 0,
    notes: "Generated user #{i}"
  }
end)

:ok = MyApp.Person.stream_to_file("users.csv", stream)
```

## Field Types

Delimit supports the following field types:

| Type        | Description            | Example                        |
| ----------- | ---------------------- | ------------------------------ |
| `:string`   | Basic string values    | `field :name, :string`         |
| `:integer`  | Integer numbers        | `field :age, :integer`         |
| `:float`    | Floating point numbers | `field :salary, :float`        |
| `:boolean`  | Boolean values         | `field :active, :boolean`      |
| `:date`     | Date values            | `field :birthday, :date`       |
| `:datetime` | DateTime values        | `field :created_at, :datetime` |

## Field Options

Each field can have additional options:

```elixir
# Default value when field is missing
field :age, :integer, default: 0

# Custom header name in CSV file
field :email, :string, label: "contact_email"

# Format for date/datetime fields
field :birthday, :date, format: "MM/DD/YYYY"

# Convert empty strings to nil
field :notes, :string, nil_on_empty: true

# Custom values for boolean fields
field :status, :boolean, true_values: ["Y", "Yes"], false_values: ["N", "No"]

# Custom conversion functions with explicit struct type
field :tags, :string,
  read_fn: &String.split(&1, "|"),
  write_fn: &Enum.join(&1, "|"),
  struct_type: {:list, :string}
```

## Advanced Usage

This section covers more advanced features and techniques for getting the most out of Delimit.

### Type Specifications

Delimit automatically generates typespecs for your schemas, including support for complex field types:

```elixir
defmodule MyApp.User do
  use Delimit

  layout do
    field :name, :string
    # File contains comma-separated tags, but in memory it's a list
    field :tags, :string,
      read_fn: &String.split(&1, ","),
      write_fn: &Enum.join(&1, ","),
      struct_type: {:list, :string}

    # Map type with string keys and integer values
    field :scores, :string,
      read_fn: &parse_scores/1,
      write_fn: &serialize_scores/1,
      struct_type: {:map, :string, :integer}
  end

  defp parse_scores(str), do: # Parse string to map
  defp serialize_scores(map), do: # Convert map to string
end
```

### Embedded Schemas

You can nest schemas using the `embeds_one` macro:

```elixir
defmodule MyApp.Address do
  use Delimit

  layout do
    field :street, :string
    field :city, :string
    field :state, :string
    field :postal_code, :string
  end
end

defmodule MyApp.Customer do
  use Delimit

  layout do
    field :name, :string
    field :email, :string
    embeds_one :address, MyApp.Address
    embeds_one :billing_address, MyApp.Address, prefix: "billing_"
  end
end

# This will handle headers like:
# name,email,street,city,state,postal_code,billing_street,billing_city,billing_state,billing_postal_code
#
# And create structs like:
# %MyApp.Customer{
#   name: "John Doe",
#   email: "john@example.com",
#   address: %MyApp.Address{street: "123 Main St", ...},
#   billing_address: %MyApp.Address{street: "456 Billing St", ...}
# }
```

### Using Standard Formats

Delimit provides built-in support for common file formats:

```elixir
# Read tab-separated values with the format option
people = MyApp.Person.read("people.tsv", format: :tsv)

# Read comma-separated values (also the default)
people = MyApp.Person.read("people.csv", format: :csv)

# Write pipe-separated values
:ok = MyApp.Person.write("people.psv", people, format: :psv)
```

Supported formats include:

- `:csv` - Comma-separated values with double-quote escaping
- `:tsv` - Tab-separated values with double-quote escaping
- `:psv` - Pipe-separated values with double-quote escaping
- `:ssv` - Semi-colon-separated values with double-quote escaping

### Parser Configuration Options

Delimit provides several customization options for parsing and generating delimited files:

#### Delimiter Options

```elixir
# Read tab-separated values with explicit delimiter
people = MyApp.Person.read("people.tsv", delimiter: "\t")

# Write pipe-separated values with explicit delimiter
:ok = MyApp.Person.write("people.psv", people, delimiter: "|")

# Use a specific escape character (default is double-quote)
people = MyApp.Person.read("people.csv", escape: "\"")

# Set line ending for generated files (default is \n)
:ok = MyApp.Person.write("people.csv", people, line_ending: "\r\n")
```

#### Headers and Content Processing

```elixir
# Control whether headers are included (default is true)
people = MyApp.Person.read("people.csv", headers: false)
:ok = MyApp.Person.write("people.csv", people, headers: false)

# Skip a specific number of lines at the beginning of a file
people = MyApp.Person.read("people.csv", skip_lines: 3)

# Skip lines dynamically based on content (like comments)
people = MyApp.Person.read("people.csv",
  skip_while: fn line -> String.starts_with?(line, "#") end)

# Control whether to trim whitespace from fields
people = MyApp.Person.read("people.csv", trim_fields: true)
```

#### Combining Multiple Options

Options can be combined for complete customization:

```elixir
# Multiple options can be combined
people = MyApp.Person.read("people.csv",
  delimiter: ";",
  escape: "\"",
  skip_lines: 2,
  trim_fields: true,
  headers: true
)
```

## License

This project is licensed under the LGPL-3 License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
