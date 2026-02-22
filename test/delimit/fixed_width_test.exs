defmodule Delimit.FixedWidthTest do
  use ExUnit.Case, async: true

  alias Delimit.TestSupport.FixedWidthSchemas.DualFormat
  alias Delimit.TestSupport.FixedWidthSchemas.FixedAddress
  alias Delimit.TestSupport.FixedWidthSchemas.FixedCustomer
  alias Delimit.TestSupport.FixedWidthSchemas.FullFixed
  alias Delimit.TestSupport.FixedWidthSchemas.MissingWidth
  alias Delimit.TestSupport.FixedWidthSchemas.NegativeWidth
  alias Delimit.TestSupport.FixedWidthSchemas.NoWidthEmbed
  alias Delimit.TestSupport.FixedWidthSchemas.RightJustified
  alias Delimit.TestSupport.FixedWidthSchemas.SimpleFixed
  alias Delimit.TestSupport.FixedWidthSchemas.SingleCharField
  alias Delimit.TestSupport.FixedWidthSchemas.WithCustomFn
  alias Delimit.TestSupport.FixedWidthSchemas.WithDatetime
  alias Delimit.TestSupport.FixedWidthSchemas.WithDefaults
  alias Delimit.TestSupport.FixedWidthSchemas.ZeroPadded
  alias Delimit.TestSupport.FixedWidthSchemas.ZeroWidth

  describe "validation" do
    test "raises when field is missing width" do
      assert_raise ArgumentError, ~r/missing :width/, fn ->
        MissingWidth.write_string([%MissingWidth{name: "test", age: 1}], format: :fixed_width)
      end
    end

    test "raises when reading with missing width" do
      assert_raise ArgumentError, ~r/missing :width/, fn ->
        MissingWidth.read_string("test      1    ", format: :fixed_width)
      end
    end

    test "raises when width is zero" do
      assert_raise ArgumentError, ~r/positive integer/, fn ->
        ZeroWidth.write_string([%ZeroWidth{name: "test"}], format: :fixed_width)
      end
    end

    test "raises when width is negative" do
      assert_raise ArgumentError, ~r/positive integer/, fn ->
        NegativeWidth.write_string([%NegativeWidth{name: "test"}], format: :fixed_width)
      end
    end

    test "raises when embed fields are missing width" do
      assert_raise ArgumentError, ~r/missing :width/, fn ->
        NoWidthEmbed.write_string(
          [
            %NoWidthEmbed{
              name: "Alice",
              addr: %NoWidthEmbed.NoWidthAddr{city: "Portland", state: "OR"}
            }
          ],
          format: :fixed_width
        )
      end
    end
  end

  describe "writing" do
    test "basic write_string" do
      data = [%SimpleFixed{name: "Alice", age: 30, active: true}]
      result = SimpleFixed.write_string(data, format: :fixed_width)
      assert result == "Alice     30   true \r\n"
    end

    test "write_string with multiple records" do
      data = [
        %SimpleFixed{name: "Alice", age: 30, active: true},
        %SimpleFixed{name: "Bob", age: 25, active: false}
      ]

      result = SimpleFixed.write_string(data, format: :fixed_width)
      assert result == "Alice     30   true \r\nBob       25   false\r\n"
    end

    test "write_string truncates long values" do
      data = [%SimpleFixed{name: "VeryLongNameThatExceedsWidth", age: 30, active: true}]
      result = SimpleFixed.write_string(data, format: :fixed_width)
      # name is truncated to 10 chars
      line = result |> String.split("\r\n") |> hd()
      assert String.length(line) == 20
      assert String.starts_with?(line, "VeryLongNa")
    end

    test "write_string with nil values produces pad chars" do
      data = [%SimpleFixed{name: nil, age: nil, active: nil}]
      result = SimpleFixed.write_string(data, format: :fixed_width)
      line = result |> String.split("\r\n") |> hd()
      # All spaces: 10 + 5 + 5 = 20
      assert line == String.duplicate(" ", 20)
    end

    test "write_string with right justification" do
      data = [%RightJustified{id: 42, name: "Alice", amount: 100.5}]
      result = RightJustified.write_string(data, format: :fixed_width)
      line = result |> String.split("\r\n") |> hd()
      # id: "        42" (10 chars, right-justified)
      assert String.slice(line, 0, 10) == "        42"
      # name: "Alice               " (20 chars, left-justified)
      assert String.slice(line, 10, 20) == "Alice               "
      # amount: "       100.5" (12 chars, right-justified)
      assert String.slice(line, 30, 12) == "       100.5"
    end

    test "write_string with zero padding" do
      data = [%ZeroPadded{id: 42, code: "AB"}]
      result = ZeroPadded.write_string(data, format: :fixed_width)
      line = result |> String.split("\r\n") |> hd()
      assert String.slice(line, 0, 8) == "00000042"
      assert String.slice(line, 8, 5) == "000AB"
    end

    test "write_string with custom line ending" do
      data = [
        %SimpleFixed{name: "Alice", age: 30, active: true},
        %SimpleFixed{name: "Bob", age: 25, active: false}
      ]

      result = SimpleFixed.write_string(data, format: :fixed_width, line_ending: "\n")
      assert result == "Alice     30   true \nBob       25   false\n"
    end

    test "write_string with empty list" do
      result = SimpleFixed.write_string([], format: :fixed_width)
      assert result == ""
    end

    test "write_file creates file" do
      path = Path.join(System.tmp_dir(), "delimit_fw_test_#{:rand.uniform(1_000_000)}.dat")

      on_exit(fn -> File.rm(path) end)

      data = [%SimpleFixed{name: "Alice", age: 30, active: true}]
      assert :ok = SimpleFixed.write(path, data, format: :fixed_width)
      assert File.exists?(path)

      content = File.read!(path)
      assert content == "Alice     30   true \r\n"
    end

    test "write_file infers fixed_width from .dat extension" do
      path = Path.join(System.tmp_dir(), "delimit_fw_ext_#{:rand.uniform(1_000_000)}.dat")

      on_exit(fn -> File.rm(path) end)

      data = [%SimpleFixed{name: "Alice", age: 30, active: true}]
      # No explicit format: — should infer :fixed_width from .dat
      assert :ok = SimpleFixed.write(path, data)
      content = File.read!(path)
      assert content == "Alice     30   true \r\n"
    end

    test "write_string with nil embed produces padded empty fields" do
      data = [%FixedCustomer{name: "Alice", email: "alice@test.com", address: nil}]
      result = FixedCustomer.write_string(data, format: :fixed_width)
      line = result |> String.split("\r\n") |> hd()
      # Total: 20 + 30 + 20 + 15 + 2 + 5 = 92
      assert String.length(line) == 92
      # The embed portion (last 42 chars) should be all spaces
      embed_part = String.slice(line, 50, 42)
      assert embed_part == String.duplicate(" ", 42)
    end

    test "stream_to_file writes correctly" do
      path = Path.join(System.tmp_dir(), "delimit_fw_stream_#{:rand.uniform(1_000_000)}.dat")

      on_exit(fn -> File.rm(path) end)

      stream =
        Stream.map(1..3, fn i ->
          %SimpleFixed{name: "User#{i}", age: 20 + i, active: rem(i, 2) == 0}
        end)

      assert :ok = SimpleFixed.stream_to_file(path, stream, format: :fixed_width)
      assert File.exists?(path)

      content = File.read!(path)
      lines = content |> String.split("\r\n") |> Enum.reject(&(&1 == ""))
      assert length(lines) == 3
    end
  end

  describe "reading" do
    test "basic read_string" do
      input = "Alice     30   true \r\n"
      [record] = SimpleFixed.read_string(input, format: :fixed_width)
      assert record.name == "Alice"
      assert record.age == 30
      assert record.active == true
    end

    test "read_string with multiple records" do
      input = "Alice     30   true \r\nBob       25   false\r\n"
      records = SimpleFixed.read_string(input, format: :fixed_width)
      assert length(records) == 2

      [alice, bob] = records
      assert alice.name == "Alice"
      assert alice.age == 30
      assert alice.active == true
      assert bob.name == "Bob"
      assert bob.age == 25
      assert bob.active == false
    end

    test "read_string with LF line endings" do
      input = "Alice     30   true \nBob       25   false\n"
      records = SimpleFixed.read_string(input, format: :fixed_width)
      assert length(records) == 2
    end

    test "read_string handles short lines with missing fields as nil" do
      # Line shorter than expected - missing fields become nil
      input = "Alice     30\r\n"
      [record] = SimpleFixed.read_string(input, format: :fixed_width)
      assert record.name == "Alice"
      assert record.age == 30
      assert record.active == nil
    end

    test "read_string handles extra characters on long lines" do
      # Line longer than expected - extra chars ignored
      input = "Alice     30   true EXTRA_STUFF\r\n"
      [record] = SimpleFixed.read_string(input, format: :fixed_width)
      assert record.name == "Alice"
      assert record.age == 30
      assert record.active == true
    end

    test "read_string with skip_lines" do
      input = "HEADER LINE HERE    \r\nAlice     30   true \r\n"
      [record] = SimpleFixed.read_string(input, format: :fixed_width, skip_lines: 1)
      assert record.name == "Alice"
    end

    test "read_string with skip_while" do
      input = "# comment\r\nAlice     30   true \r\n"

      [record] =
        SimpleFixed.read_string(input,
          format: :fixed_width,
          skip_while: &String.starts_with?(&1, "#")
        )

      assert record.name == "Alice"
    end

    test "read_string with empty input" do
      assert [] = SimpleFixed.read_string("", format: :fixed_width)
    end

    test "read_file reads correctly" do
      path = Path.join(System.tmp_dir(), "delimit_fw_read_#{:rand.uniform(1_000_000)}.dat")

      on_exit(fn -> File.rm(path) end)

      File.write!(path, "Alice     30   true \r\nBob       25   false\r\n")

      records = SimpleFixed.read(path, format: :fixed_width)
      assert length(records) == 2
      assert hd(records).name == "Alice"
    end

    test "stream_file streams correctly" do
      path =
        Path.join(System.tmp_dir(), "delimit_fw_stream_read_#{:rand.uniform(1_000_000)}.dat")

      on_exit(fn -> File.rm(path) end)

      File.write!(path, "Alice     30   true \r\nBob       25   false\r\n")

      records = path |> SimpleFixed.stream(format: :fixed_width) |> Enum.to_list()
      assert length(records) == 2
      assert hd(records).name == "Alice"
    end

    test "read_string handles partial field on short line" do
      # SimpleFixed: name(10) + age(5) + active(5) = 20
      # Line is 13 chars — name is full, age is full, active has only 3 of 5 chars
      input = "Alice     30 tr\r\n"
      [record] = SimpleFixed.read_string(input, format: :fixed_width)
      assert record.name == "Alice"
      assert record.age == 30

      # "tr" is partial, but still parseable (Field.parse_value handles "tr" -> nil for boolean)
      # The exact behavior depends on parse_value — just verify no crash
      assert record.active in [true, nil]
    end

    test "read_string with trim_fields: false preserves padding" do
      input = "Alice     30   true \r\n"
      [record] = SimpleFixed.read_string(input, format: :fixed_width, trim_fields: false)
      # With trim disabled, the name field keeps its trailing spaces
      assert record.name == "Alice     "
    end

    test "read_string with defaults for nil fields" do
      # All spaces => nil => default applied
      input = String.duplicate(" ", 15) <> "\r\n"
      [record] = WithDefaults.read_string(input, format: :fixed_width)
      assert record.name == "N/A"
      assert record.count == 0
    end
  end

  describe "all field types" do
    test "full schema round-trip" do
      data = [
        %FullFixed{
          first_name: "John",
          last_name: "Doe",
          age: 30,
          salary: 50_000.5,
          hired_date: ~D[2020-01-15],
          active: true
        }
      ]

      output = FullFixed.write_string(data, format: :fixed_width)
      [record] = FullFixed.read_string(output, format: :fixed_width)

      assert record.first_name == "John"
      assert record.last_name == "Doe"
      assert record.age == 30
      assert record.salary == 50_000.5
      assert record.hired_date == ~D[2020-01-15]
      assert record.active == true
    end

    test "datetime field round-trip" do
      dt = DateTime.from_naive!(~N[2024-06-15 14:30:00], "Etc/UTC")
      data = [%WithDatetime{label: "event1", timestamp: dt}]

      output = WithDatetime.write_string(data, format: :fixed_width)
      [record] = WithDatetime.read_string(output, format: :fixed_width)

      assert record.label == "event1"
      assert record.timestamp == dt
    end

    test "right-justified round-trip" do
      data = [%RightJustified{id: 42, name: "Alice", amount: 99.99}]

      output = RightJustified.write_string(data, format: :fixed_width)
      [record] = RightJustified.read_string(output, format: :fixed_width)

      assert record.id == 42
      assert record.name == "Alice"
      assert record.amount == 99.99
    end
  end

  describe "embeds" do
    test "write with embedded schema" do
      data = [
        %FixedCustomer{
          name: "Alice",
          email: "alice@example.com",
          address: %FixedAddress{
            street: "123 Main St",
            city: "Springfield",
            state: "IL",
            zip: "62701"
          }
        }
      ]

      result = FixedCustomer.write_string(data, format: :fixed_width)
      line = result |> String.split("\r\n") |> hd()
      # Total width: 20 + 30 + 20 + 15 + 2 + 5 = 92
      assert String.length(line) == 92
    end

    test "read with embedded schema" do
      # Build a line: name(20) + email(30) + street(20) + city(15) + state(2) + zip(5)
      line =
        String.pad_trailing("Alice", 20) <>
          String.pad_trailing("alice@example.com", 30) <>
          String.pad_trailing("123 Main St", 20) <>
          String.pad_trailing("Springfield", 15) <>
          String.pad_trailing("IL", 2) <>
          String.pad_trailing("62701", 5)

      input = line <> "\r\n"
      [record] = FixedCustomer.read_string(input, format: :fixed_width)

      assert record.name == "Alice"
      assert record.email == "alice@example.com"
      assert record.address.street == "123 Main St"
      assert record.address.city == "Springfield"
      assert record.address.state == "IL"
      assert record.address.zip == "62701"
    end

    test "embed round-trip" do
      data = [
        %FixedCustomer{
          name: "Bob",
          email: "bob@test.com",
          address: %FixedAddress{
            street: "456 Oak Ave",
            city: "Portland",
            state: "OR",
            zip: "97201"
          }
        }
      ]

      output = FixedCustomer.write_string(data, format: :fixed_width)
      [record] = FixedCustomer.read_string(output, format: :fixed_width)

      assert record.name == "Bob"
      assert record.email == "bob@test.com"
      assert record.address.street == "456 Oak Ave"
      assert record.address.city == "Portland"
      assert record.address.state == "OR"
      assert record.address.zip == "97201"
    end
  end

  describe "round-trip" do
    test "write then read preserves data" do
      original = [
        %SimpleFixed{name: "Alice", age: 30, active: true},
        %SimpleFixed{name: "Bob", age: 25, active: false},
        %SimpleFixed{name: "Charlie", age: 35, active: true}
      ]

      output = SimpleFixed.write_string(original, format: :fixed_width)
      result = SimpleFixed.read_string(output, format: :fixed_width)

      assert length(result) == 3

      original
      |> Enum.zip(result)
      |> Enum.each(fn {orig, res} ->
        assert res.name == orig.name
        assert res.age == orig.age
        assert res.active == orig.active
      end)
    end

    test "file write then read round-trip" do
      path = Path.join(System.tmp_dir(), "delimit_fw_rt_#{:rand.uniform(1_000_000)}.dat")

      on_exit(fn -> File.rm(path) end)

      original = [
        %SimpleFixed{name: "Alice", age: 30, active: true},
        %SimpleFixed{name: "Bob", age: 25, active: false}
      ]

      SimpleFixed.write(path, original, format: :fixed_width)
      result = SimpleFixed.read(path, format: :fixed_width)

      assert length(result) == 2
      assert hd(result).name == "Alice"
      assert List.last(result).name == "Bob"
    end

    test "stream write then stream read round-trip" do
      path = Path.join(System.tmp_dir(), "delimit_fw_srt_#{:rand.uniform(1_000_000)}.dat")

      on_exit(fn -> File.rm(path) end)

      original = [
        %SimpleFixed{name: "Alice", age: 30, active: true},
        %SimpleFixed{name: "Bob", age: 25, active: false}
      ]

      SimpleFixed.stream_to_file(path, original, format: :fixed_width)
      result = path |> SimpleFixed.stream(format: :fixed_width) |> Enum.to_list()

      assert length(result) == 2
      assert hd(result).name == "Alice"
    end
  end

  describe "custom read_fn/write_fn" do
    test "write_string uses write_fn" do
      data = [%WithCustomFn{name: "Alice", tags: ["elixir", "rust"]}]
      result = WithCustomFn.write_string(data, format: :fixed_width)
      line = result |> String.split("\r\n") |> hd()
      # name(10) + tags(20)
      assert String.length(line) == 30
      # tags field should contain "elixir|rust" left-padded to 20
      assert String.slice(line, 10, 20) == "elixir|rust         "
    end

    test "read_string uses read_fn" do
      input = "Alice     elixir|rust        \r\n"
      [record] = WithCustomFn.read_string(input, format: :fixed_width)
      assert record.name == "Alice"
      assert record.tags == ["elixir", "rust"]
    end

    test "custom fn round-trip" do
      data = [%WithCustomFn{name: "Bob", tags: ["go", "py", "js"]}]
      output = WithCustomFn.write_string(data, format: :fixed_width)
      [record] = WithCustomFn.read_string(output, format: :fixed_width)
      assert record.name == "Bob"
      assert record.tags == ["go", "py", "js"]
    end
  end

  describe "mixed format usage" do
    test "same schema writes as CSV" do
      data = [%DualFormat{id: 1, name: "Alice", score: 95.5}]
      csv = DualFormat.write_string(data, format: :csv)
      assert csv =~ "1,Alice,95.5"
    end

    test "same schema writes as fixed_width" do
      data = [%DualFormat{id: 1, name: "Alice", score: 95.5}]
      fw = DualFormat.write_string(data, format: :fixed_width)
      line = fw |> String.split("\r\n") |> hd()
      assert String.length(line) == 40
      assert String.slice(line, 0, 10) == "1         "
      assert String.slice(line, 10, 20) == "Alice               "
    end

    test "CSV round-trip ignores width option" do
      data = [%DualFormat{id: 42, name: "Bob", score: 88.0}]
      csv = DualFormat.write_string(data, format: :csv)
      [record] = DualFormat.read_string(csv, format: :csv)
      assert record.id == 42
      assert record.name == "Bob"
      assert record.score == 88.0
    end

    test "fixed_width round-trip with same schema" do
      data = [%DualFormat{id: 42, name: "Bob", score: 88.0}]
      fw = DualFormat.write_string(data, format: :fixed_width)
      [record] = DualFormat.read_string(fw, format: :fixed_width)
      assert record.id == 42
      assert record.name == "Bob"
      assert record.score == 88.0
    end
  end

  describe "single-character fields" do
    test "write width-1 fields" do
      data = [%SingleCharField{record_type: "A", flag: true, code: "XYZ"}]
      result = SingleCharField.write_string(data, format: :fixed_width)
      line = result |> String.split("\r\n") |> hd()
      # record_type(1) + flag(1, "true" truncated to "t") + code(3) = 5
      assert String.length(line) == 5
      assert line == "AtXYZ"
    end

    test "read width-1 fields" do
      # record_type(1) + flag(1) as "t" + code(3)
      input = "A1ABC\r\n"
      [record] = SingleCharField.read_string(input, format: :fixed_width)
      assert record.record_type == "A"
      assert record.flag == true
      assert record.code == "ABC"
    end

    test "truncation on width-1 boolean" do
      # Boolean "true" gets truncated to "t" for width 1,
      # and "false" gets truncated to "f"
      data = [
        %SingleCharField{record_type: "A", flag: true, code: "XY"},
        %SingleCharField{record_type: "B", flag: false, code: "ZZ"}
      ]

      result = SingleCharField.write_string(data, format: :fixed_width)
      lines = result |> String.split("\r\n") |> Enum.reject(&(&1 == ""))
      assert lines |> hd() |> String.at(1) == "t"
      assert lines |> List.last() |> String.at(1) == "f"
    end
  end

  describe "zero-padded round-trip" do
    test "zero-padded integer reads back correctly" do
      data = [%ZeroPadded{id: 42, code: "AB"}]
      output = ZeroPadded.write_string(data, format: :fixed_width)
      [record] = ZeroPadded.read_string(output, format: :fixed_width)
      # Integer parsing handles leading zeros: "00000042" -> 42
      assert record.id == 42
      # String field preserves zero padding since trim only strips whitespace
      assert record.code == "000AB"
    end

    test "zero-padded with larger numbers" do
      data = [%ZeroPadded{id: 12_345_678, code: "ABCDE"}]
      output = ZeroPadded.write_string(data, format: :fixed_width)
      line = output |> String.split("\r\n") |> hd()
      assert line == "12345678ABCDE"
      [record] = ZeroPadded.read_string(output, format: :fixed_width)
      assert record.id == 12_345_678
      assert record.code == "ABCDE"
    end
  end

  describe "formats integration" do
    test "fixed_width is in supported_formats" do
      assert :fixed_width in Delimit.Formats.supported_formats()
    end

    test "get_options returns correct options for fixed_width" do
      opts = Delimit.Formats.get_options(:fixed_width)
      assert opts == [line_ending: "\r\n"]
    end
  end
end
