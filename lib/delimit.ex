defmodule Delimit do
  @moduledoc """
  Delimit: A library for defining and working with delimited data files.

  Delimit allows you to define a schema for delimited data files (CSV, TSV, etc.)
  and provides functions for reading, writing, and manipulating that data.

  ## Example

      defmodule MyApp.Person do
        use Delimit

        layout do
          field :first_name, :string
          field :last_name, :string
          field :age, :integer
          field :birthday, :date, format: "YYYY-MM-DD"
          field :active, :boolean
        end
      end

      # Read data from a file
      people = MyApp.Person.read("people.csv")

      # Write data to a file
      MyApp.Person.write("new_people.csv", people)

      # Work with a specific record
      first_person = Enum.at(people, 0)
      IO.puts("Name: \#{first_person.first_name} \#{first_person.last_name}")
  """

  # Module aliases can be added here as needed

  @doc false
  defmacro __using__(_opts) do
    quote do
      import Delimit, only: [layout: 1]
      
      @delimit_schema Delimit.Schema.new(__MODULE__)
      
      @before_compile Delimit
    end
  end

  @doc """
  Defines the layout of a delimited file.

  ## Example

      layout do
        field :first_name, :string
        field :last_name, :string
        field :age, :integer
      end
  """
  defmacro layout(do: block) do
    quote do
      import Delimit, only: [field: 2, field: 3, embeds_one: 2, embeds_one: 3]
      
      unquote(block)
      
      import Delimit, only: []
    end
  end

  @doc """
  Defines a field in the schema.

  ## Parameters

    * `name` - The name of the field as atom
    * `type` - The data type of the field (:string, :integer, :float, etc.)

  ## Example

      field :first_name, :string
  """
  defmacro field(name, type) do
    quote do
      @delimit_schema Delimit.Schema.add_field(@delimit_schema, unquote(name), unquote(type))
    end
  end

  @doc """
  Defines a field in the schema with options.

  ## Parameters

    * `name` - The name of the field as atom
    * `type` - The data type of the field (:string, :integer, :float, etc.)
    * `opts` - Options for the field

  ## Options

    * `:format` - Format string for date/time fields
    * `:default` - Default value if the field is missing
    * `:nil_on_empty` - If true, empty strings become nil (default: true)
    * `:true_values` - List of values to interpret as true for boolean fields
    * `:false_values` - List of values to interpret as false for boolean fields
    * `:read_fn` - Custom function to parse the raw field value
    * `:write_fn` - Custom function to convert the field value to string
    * `:label` - Custom header label for this field (instead of the field name)

  ## Example

      field :birthday, :date, format: "YYYY-MM-DD"
      field :active, :boolean, true_values: ["Y", "YES"], false_values: ["N", "NO"]
      field :email, :string, label: "contact_email"
  """
  defmacro field(name, type, opts) do
    quote do
      @delimit_schema Delimit.Schema.add_field(@delimit_schema, unquote(name), unquote(type), unquote(opts))
    end
  end

  @doc """
  Defines an embedded schema.

  ## Parameters

    * `name` - The name for the embedded schema
    * `module` - The module defining the embedded schema

  ## Example

      embeds_one :address, MyApp.Address
      embeds_one :billing_address, MyApp.Address, prefix: "billing"
  """
  defmacro embeds_one(name, module) do
    quote do
      @delimit_schema Delimit.Schema.add_embed(@delimit_schema, unquote(name), unquote(module))
    end
  end

  @doc """
  Defines an embedded schema with options.

  ## Parameters

    * `name` - The name for the embedded schema
    * `module` - The module defining the embedded schema
    * `opts` - Options for the embedded schema

  ## Options

    * `:prefix` - Prefix to add to field headers (default: field name + "_")

  ## Example

      embeds_one :address, MyApp.Address, prefix: "addr_"
  """
  defmacro embeds_one(name, module, opts) do
    quote do
      @delimit_schema Delimit.Schema.add_embed(@delimit_schema, unquote(name), unquote(module), unquote(opts))
    end
  end

  @doc false
  defmacro __before_compile__(_env) do
    quote do
      # Store the schema in a module attribute accessible at runtime
      def __delimit_schema__ do
        @delimit_schema
      end
      
      @doc """
      Reads delimited data from a file.

      ## Parameters

        * `path` - Path to the delimited file
        * `opts` - Options for reading (headers, delimiter, etc.)

      ## Returns

        * List of parsed records based on the schema

      ## Examples

          iex> MyApp.Person.read("people.csv")
          [%{first_name: "John", last_name: "Doe", age: 42}, ...]
      """
      @spec read(Path.t(), Keyword.t()) :: [map()]
      def read(path, opts \\ []) do
        Delimit.Reader.read_file(__delimit_schema__(), path, opts)
      end
      
      @doc """
      Reads delimited data from a string.

      ## Parameters

        * `string` - String containing delimited data
        * `opts` - Options for reading (headers, delimiter, etc.)

      ## Returns

        * List of parsed records based on the schema

      ## Examples

          iex> csv = "first_name,last_name\\nJohn,Doe"
          iex> MyApp.Person.read_string(csv)
          [%{first_name: "John", last_name: "Doe"}]
      """
      @spec read_string(binary(), Keyword.t()) :: [map()]
      def read_string(string, opts \\ []) do
        Delimit.Reader.read_string(__delimit_schema__(), string, opts)
      end
      
      @doc """
      Streams delimited data from a file.

      ## Parameters

        * `path` - Path to the delimited file
        * `opts` - Options for reading (headers, delimiter, etc.)

      ## Returns

        * Stream of parsed records based on the schema

      ## Examples

          iex> MyApp.Person.stream("large_file.csv")
          iex> |> Stream.take(10)
          iex> |> Enum.to_list()
          [%{first_name: "John", last_name: "Doe"}, ...]
      """
      @spec stream(Path.t(), Keyword.t()) :: Enumerable.t()
      def stream(path, opts \\ []) do
        Delimit.Reader.stream_file(__delimit_schema__(), path, opts)
      end
      
      @doc """
      Writes delimited data to a file.

      ## Parameters

        * `path` - Path to the output file
        * `data` - List of structs or maps to write
        * `opts` - Options for writing (headers, delimiter, etc.)

      ## Returns

        * `:ok` on success

      ## Examples

          iex> people = [%{first_name: "John", last_name: "Doe"}]
          iex> MyApp.Person.write("people.csv", people)
          :ok
      """
      @spec write(Path.t(), [map()], Keyword.t()) :: :ok
      def write(path, data, opts \\ []) do
        Delimit.Writer.write_file(__delimit_schema__(), path, data, opts)
      end
      
      @doc """
      Writes delimited data to a string.

      ## Parameters

        * `data` - List of structs or maps to write
        * `opts` - Options for writing (headers, delimiter, etc.)

      ## Returns

        * String containing the delimited data

      ## Examples

          iex> people = [%{first_name: "John", last_name: "Doe"}]
          iex> MyApp.Person.write_string(people)
          "first_name,last_name\\nJohn,Doe\\n"
      """
      @spec write_string([map()], Keyword.t()) :: binary()
      def write_string(data, opts \\ []) do
        Delimit.Writer.write_string(__delimit_schema__(), data, opts)
      end
      
      @doc """
      Streams delimited data to a file.

      ## Parameters

        * `path` - Path to the output file
        * `data_stream` - Stream of structs or maps to write
        * `opts` - Options for writing (headers, delimiter, etc.)

      ## Returns

        * `:ok` on success

      ## Examples

          iex> stream = Stream.map(1..100, &(%{first_name: "User \#{&1}"}))
          iex> MyApp.Person.stream_to_file("people.csv", stream)
          :ok
      """
      @spec stream_to_file(Path.t(), Enumerable.t(), Keyword.t()) :: :ok
      def stream_to_file(path, data_stream, opts \\ []) do
        Delimit.Writer.stream_to_file(__delimit_schema__(), path, data_stream, opts)
      end
    end
  end
end
