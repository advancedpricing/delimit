# Delimit

[![Hex.pm Version](https://img.shields.io/hexpm/v/delimit.svg)](https://hex.pm/packages/delimit)
[![Hex.pm License](https://img.shields.io/hexpm/l/delimit.svg)](https://github.com/jcowgar/delimit/blob/main/LICENSE)

Delimit is a powerful yet elegant library for reading and writing delimited and fixed-width data files (CSV, TSV, PSV, SSV, Fixed-Width) in Elixir. Inspired by Ecto, it allows you to define schemas for your delimited data, providing strong typing with structs, validation, and transformation capabilities. By defining the structure of your data, Delimit enables type-safe parsing and generation with minimal boilerplate code.

## Features

- **Schema-based approach**: Define the structure of your delimited files using Ecto-like schemas
- **Strong typing with structs**: Convert between string values and proper Elixir types in type-safe structs
- **Full TypeSpecs**: Automatically generated type specifications for your schemas
- **Streaming support**: Process large files efficiently with Elixir streams
- **Fixed-width format**: Read and write fixed-width data with configurable field widths, padding, and justification
- **Customizable parsing**: Configure delimiters, headers, type conversion, and more
- **Embedded schemas**: Nest schemas for complex data structures
- **Custom transformations**: Add your own read/write functions for special data formats
- **Memory efficient**: Stream large files without loading everything into memory

## Installation

Add `delimit` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:delimit, "~> 0.3.0"}
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
    field :birthday, :date, format: "{YYYY}-{0M}-{0D}"
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

> **Note:** Date and DateTime fields use [Timex](https://hexdocs.pm/timex/Timex.Format.DateTime.Formatters.Default.html) format patterns for parsing and formatting.

```elixir
# Default value when field is missing
field :age, :integer, default: 0

# Custom header name in CSV file
field :email, :string, label: "contact_email"

# Format for date/datetime fields (using Timex format patterns)
field :birthday, :date, format: "{0M}/{0D}/{YYYY}"

# Multiple formats — tries each in order, first to parse wins (useful for
# files with mixed formats). Writing uses the first entry.
field :birthday, :date, formats: ["{M}/{D}/{YYYY}", "{YYYY}-{0M}-{0D}"]

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
- `:fixed_width` - Fixed-width fields defined by character width (see below)

### Fixed-Width Format

Fixed-width format uses character widths instead of delimiters. Each field occupies a specific number of characters per line. Define fields with the `:width` option:

```elixir
defmodule MyApp.LegacyRecord do
  use Delimit

  layout do
    field :id, :integer, width: 8, justify: :right, pad_char: "0"
    field :name, :string, width: 20
    field :amount, :float, width: 12, justify: :right
    field :active, :boolean, width: 5
  end
end
```

Reading and writing works the same as delimited formats:

```elixir
# Read from a file
records = MyApp.LegacyRecord.read("data.dat", format: :fixed_width)

# Read from a string
data = "00000001John Doe                 50000.00true "
records = MyApp.LegacyRecord.read_string(data, format: :fixed_width)

# Write to a file
:ok = MyApp.LegacyRecord.write("output.dat", records, format: :fixed_width)

# Stream support
MyApp.LegacyRecord.stream("large_file.dat", format: :fixed_width)
|> Stream.filter(fn r -> r.active end)
|> Enum.to_list()
```

#### Fixed-Width Field Options

| Option       | Description                                      | Default |
| ------------ | ------------------------------------------------ | ------- |
| `:width`     | Number of characters the field occupies (required)| —       |
| `:justify`   | `:left` or `:right` justification when writing   | `:left` |
| `:pad_char`  | Character used to pad shorter values              | `" "`   |

Fixed-width schemas also support embedded schemas — each embedded field must also have `:width` defined on all of its fields.

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

### Derived Field Types

Delimit supports two derived field types that are populated automatically
during parsing rather than being read from a column. Both are skipped on
write and do not consume input columns.

#### `:row_hash` — Stable cryptographic hash per row

Useful for change-detection patterns, dedup-on-import flows, and audit
trails. The hash is computed from the canonical encoding of all
non-derived fields in schema order.

```elixir
defmodule MyApp.Eligibility do
  use Delimit

  layout do
    field :member_id, :string
    field :first_name, :string
    field :last_name, :string
    field :effective_date, :date

    # Hash populated automatically on read; never written.
    field :row_hash, :row_hash, algorithm: :sha256, truncate: 16
  end
end

[record | _] = MyApp.Eligibility.read("file.psv", format: :psv)
record.row_hash
# => <<16-byte binary>>

# Two structs with identical fields produce identical hashes
hash_a = MyApp.Eligibility.row_hash(record)
hash_b = MyApp.Eligibility.row_hash(%{record | first_name: record.first_name})
hash_a == hash_b  # => true
```

Options:
- `:algorithm` — any algorithm supported by `:crypto.hash/2`
  (default `:sha256`)
- `:truncate` — number of bytes to truncate to (default `16`, set `nil`
  to disable truncation)

#### `:raw_row` — Captured untyped input

Captures the row as a list of strings before any type coercion or
`read_fn` is applied. Useful for error reporting, debugging, and audit
trails.

```elixir
layout do
  field :name, :string
  field :age, :integer
  field :raw, :raw_row
end

[record] = MyApp.Person.read_string("alice|30", format: :psv)
record.raw  # => ["alice", "30"]
record.age  # => 30
```

#### Canonical encoding and row hashing for any schema

Every Delimit schema gets `canonical_string/2` and `row_hash/2` automatically:

```elixir
person = %MyApp.Person{first_name: "Alice", age: 30}

# Stable string encoding (default delimiter is ASCII Unit Separator)
MyApp.Person.canonical_string(person)
# => "Alice30"

# Or use a readable delimiter
MyApp.Person.canonical_string(person, delimiter: "|")
# => "Alice|30"

# Hash the canonical encoding
MyApp.Person.row_hash(person)
# => <<16-byte binary>>
```

These are useful even without declaring `:row_hash` as a field — for ad-hoc
comparison, logging, or building diff tooling.

## License

This project is licensed under the LGPL-3 License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
