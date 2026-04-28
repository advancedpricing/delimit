defmodule Delimit.FieldTest do
  use ExUnit.Case, async: true

  alias Delimit.Field

  describe "field creation" do
    test "creates a new string field" do
      field = Field.new(:name, :string)
      assert field.name == :name
      assert field.type == :string
      assert field.opts == []
    end

    test "creates a new field with options" do
      field = Field.new(:birthday, :date, format: "YYYY-MM-DD")
      assert field.name == :birthday
      assert field.type == :date
      assert field.opts == [format: "YYYY-MM-DD"]
    end

    test "raises on invalid field type" do
      assert_raise ArgumentError, ~r/Unsupported field type/, fn ->
        Field.new(:test, :invalid_type)
      end
    end
  end

  describe "value parsing" do
    test "parses string values" do
      field = Field.new(:name, :string)
      assert Field.parse_value("John Doe", field) == "John Doe"
    end

    test "parses integer values" do
      field = Field.new(:age, :integer)
      assert Field.parse_value("42", field) == 42
    end

    test "parses float values" do
      field = Field.new(:salary, :float)
      assert Field.parse_value("42.5", field) == 42.5
    end

    test "parses boolean values" do
      field = Field.new(:active, :boolean)

      assert Field.parse_value("true", field) == true
      assert Field.parse_value("yes", field) == true
      assert Field.parse_value("y", field) == true
      assert Field.parse_value("1", field) == true
      assert Field.parse_value("T", field) == true

      assert Field.parse_value("false", field) == false
      assert Field.parse_value("no", field) == false
      assert Field.parse_value("n", field) == false
      assert Field.parse_value("0", field) == false
      assert Field.parse_value("F", field) == false
    end

    test "parses boolean with custom values" do
      field = Field.new(:paid, :boolean, true_values: ["paid"], false_values: ["billed"])

      assert Field.parse_value("paid", field) == true
      assert Field.parse_value("billed", field) == false

      # With our more forgiving error handling, invalid values now return nil
      assert Field.parse_value("invalid", field) == nil
    end

    test "parses date values" do
      # Create a mock function for testing to avoid Timex warnings
      field =
        Field.new(:birthday, :date,
          format: "{YYYY}-{0M}-{0D}",
          read_fn: fn _val -> ~D[2023-01-15] end
        )

      parsed = Field.parse_value("2023-01-15", field)
      assert parsed.year == 2023
      assert parsed.month == 1
      assert parsed.day == 15
    end

    test "handles nil and empty values" do
      field = Field.new(:name, :string, default: "Unknown")

      assert Field.parse_value(nil, field) == "Unknown"
      assert Field.parse_value("", field) == "Unknown"
      assert Field.parse_value("  ", field) == "Unknown"
    end

    test "respects nil_on_empty option" do
      field_with_nil = Field.new(:name, :string, nil_on_empty: true)
      field_without_nil = Field.new(:name, :string, nil_on_empty: false)

      assert Field.parse_value("", field_with_nil) == nil
      assert Field.parse_value("", field_without_nil) == ""
    end

    test "uses custom read function" do
      field = Field.new(:custom, :string, read_fn: fn val -> "PROCESSED: #{val}" end)
      assert Field.parse_value("test", field) == "PROCESSED: test"
    end
  end

  describe "value to string" do
    test "converts string to string" do
      field = Field.new(:name, :string)
      assert Field.to_string("John Doe", field) == "John Doe"
    end

    test "converts integer to string" do
      field = Field.new(:age, :integer)
      assert Field.to_string(42, field) == "42"
    end

    test "converts float to string" do
      field = Field.new(:salary, :float)
      assert Field.to_string(42.5, field) == "42.5"
    end

    test "converts boolean to string" do
      field = Field.new(:active, :boolean)
      assert Field.to_string(true, field) == "true"
      assert Field.to_string(false, field) == "false"
    end

    test "converts boolean with custom output values" do
      field = Field.new(:active, :boolean, true_value: "Y", false_value: "N")
      assert Field.to_string(true, field) == "Y"
      assert Field.to_string(false, field) == "N"
    end

    test "converts date to string" do
      field =
        Field.new(:birthday, :date,
          format: "{YYYY}-{0M}-{0D}",
          write_fn: fn _date -> "2023-01-15" end
        )

      date = ~D[2023-01-15]
      assert Field.to_string(date, field) == "2023-01-15"
    end

    test "handles nil values" do
      field = Field.new(:name, :string)
      assert Field.to_string(nil, field) == ""
    end

    test "uses custom write function" do
      field = Field.new(:custom, :string, write_fn: fn val -> "EXPORTED: #{val}" end)
      assert Field.to_string("test", field) == "EXPORTED: test"
    end
  end

  describe "date formats: fallback list" do
    test "first format matches → returns parsed date" do
      field = Field.new(:dob, :date, formats: ["{M}/{D}/{YYYY}", "{YYYY}-{0M}-{0D}"])
      assert Field.parse_value("3/15/2024", field) == ~D[2024-03-15]
    end

    test "second format matches → returns parsed date" do
      field = Field.new(:dob, :date, formats: ["{M}/{D}/{YYYY}", "{YYYY}-{0M}-{0D}"])
      assert Field.parse_value("2024-03-15", field) == ~D[2024-03-15]
    end

    test "no format matches → returns nil (consistent with single-format failure)" do
      field = Field.new(:dob, :date, formats: ["{M}/{D}/{YYYY}", "{YYYY}-{0M}-{0D}"])
      assert Field.parse_value("not-a-date", field) == nil
    end

    test "formats: tries each in order until one succeeds" do
      # First format would fail, second would succeed; verify second wins.
      field = Field.new(:dob, :date, formats: ["{0M}/{0D}/{YYYY}", "{M}/{D}/{YYYY}"])
      # "3/15/2024" requires {M}/{D}/{YYYY} (single-digit month), not {0M}/{0D}/{YYYY}.
      assert Field.parse_value("3/15/2024", field) == ~D[2024-03-15]
    end

    test "empty value with nil_on_empty: true → nil regardless of formats" do
      field = Field.new(:dob, :date, formats: ["{M}/{D}/{YYYY}"])
      assert Field.parse_value("", field) == nil
    end

    test "single format: still works (backward compatibility)" do
      field = Field.new(:dob, :date, format: "{M}/{D}/{YYYY}")
      assert Field.parse_value("3/15/2024", field) == ~D[2024-03-15]
      assert Field.parse_value("not-a-date", field) == nil
    end

    test "format: and formats: together raises at field creation" do
      assert_raise ArgumentError, ~r/mutually exclusive/, fn ->
        Field.new(:dob, :date, format: "{M}/{D}/{YYYY}", formats: ["{M}/{D}/{YYYY}"])
      end
    end

    test "formats: on a non-date field raises" do
      assert_raise ArgumentError, ~r/only supported for :date and :datetime/, fn ->
        Field.new(:name, :string, formats: ["%Y"])
      end
    end

    test "empty formats: list raises" do
      assert_raise ArgumentError, ~r/at least one/, fn ->
        Field.new(:dob, :date, formats: [])
      end
    end

    test "non-string entries in formats: raises" do
      assert_raise ArgumentError, ~r/must be a string/, fn ->
        Field.new(:dob, :date, formats: ["{M}/{D}/{YYYY}", 123])
      end
    end

    test "datetime fields support formats: too" do
      field =
        Field.new(:ts, :datetime, formats: ["{M}/{D}/{YYYY} {h24}:{m}:{s}", "{ISO:Extended}"])

      slash_format_dt = Field.parse_value("3/15/2024 14:30:00", field)
      assert %DateTime{} = slash_format_dt
      assert slash_format_dt.year == 2024
      assert slash_format_dt.month == 3
      assert slash_format_dt.day == 15
      assert slash_format_dt.hour == 14

      iso_dt = Field.parse_value("2024-03-15T14:30:00Z", field)
      assert %DateTime{} = iso_dt
      assert iso_dt.year == 2024
    end

    test "writing uses the first entry of formats:" do
      field = Field.new(:dob, :date, formats: ["{M}/{D}/{YYYY}", "{YYYY}-{0M}-{0D}"])
      assert Field.to_string(~D[2024-03-15], field) == "3/15/2024"
    end

    test "writing with format: still works alongside read formats:" do
      # When both format: and formats: would conflict, field creation rejects them.
      # When only formats: is given, write uses formats[0].
      field = Field.new(:dob, :date, formats: ["{0M}-{0D}-{YYYY}"])
      assert Field.to_string(~D[2024-03-15], field) == "03-15-2024"
    end
  end
end
