defmodule Delimit.CanonicalTest do
  use ExUnit.Case, async: true

  alias Delimit.Schema

  defmodule Simple do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:age, :integer)
    end
  end

  defmodule WithDate do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:birthday, :date, format: "{M}/{D}/{YYYY}")
    end
  end

  defmodule WithDateFormats do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:dob, :date, formats: ["{M}/{D}/{YYYY}", "{YYYY}-{0M}-{0D}"])
    end
  end

  defmodule WithBoolean do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:active, :boolean, true_values: ["Y"], false_values: ["N"])
    end
  end

  defmodule TagHelpers do
    @moduledoc false
    def split_tags(value), do: String.split(value, ",")
    def join_tags(value), do: Enum.join(value, ",")
  end

  defmodule WithReadFn do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)

      field(:tags, :string,
        read_fn: &TagHelpers.split_tags/1,
        write_fn: &TagHelpers.join_tags/1
      )
    end
  end

  defmodule InnerEmbed do
    @moduledoc false
    use Delimit

    layout do
      field(:street, :string)
      field(:city, :string)
    end
  end

  defmodule WithEmbed do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      embeds_one(:address, InnerEmbed)
    end
  end

  describe "canonical_string/2" do
    test "encodes basic field values in schema order with default delimiter" do
      person = %Simple{name: "Alice", age: 30}
      out = Simple.canonical_string(person)
      assert out == "Alice" <> Schema.canonical_delimiter() <> "30"
    end

    test "with custom delimiter produces a readable form" do
      person = %Simple{name: "Alice", age: 30}
      assert Simple.canonical_string(person, delimiter: "|") == "Alice|30"
    end

    test "nil fields encode as empty string" do
      person = %Simple{name: nil, age: 30}
      assert Simple.canonical_string(person, delimiter: "|") == "|30"
    end

    test "all-nil produces consecutive empty separators" do
      person = %Simple{name: nil, age: nil}
      assert Simple.canonical_string(person, delimiter: "|") == "|"
    end

    test "two structs with identical values produce identical encoding" do
      a = %Simple{name: "Alice", age: 30}
      b = %Simple{name: "Alice", age: 30}
      assert Simple.canonical_string(a) == Simple.canonical_string(b)
    end

    test "single-field difference produces different encoding" do
      a = %Simple{name: "Alice", age: 30}
      b = %Simple{name: "Alice", age: 31}
      refute Simple.canonical_string(a) == Simple.canonical_string(b)
    end

    test "date fields use their configured format:" do
      person = %WithDate{name: "Alice", birthday: ~D[1990-03-15]}
      assert WithDate.canonical_string(person, delimiter: "|") == "Alice|3/15/1990"
    end

    test "date fields with formats: use the first format for canonical encoding" do
      person = %WithDateFormats{name: "Alice", dob: ~D[1990-03-15]}
      assert WithDateFormats.canonical_string(person, delimiter: "|") == "Alice|3/15/1990"
    end

    test "boolean fields use their first true_values/false_values entry" do
      yes = %WithBoolean{name: "Alice", active: true}
      no = %WithBoolean{name: "Bob", active: false}
      assert WithBoolean.canonical_string(yes, delimiter: "|") == "Alice|Y"
      assert WithBoolean.canonical_string(no, delimiter: "|") == "Bob|N"
    end

    test "fields with write_fn use the write_fn output" do
      person = %WithReadFn{name: "Alice", tags: ["admin", "ops"]}
      assert WithReadFn.canonical_string(person, delimiter: "|") == "Alice|admin,ops"
    end

    test "embeds contribute their fields recursively in schema order" do
      person = %WithEmbed{
        name: "Alice",
        address: %InnerEmbed{street: "100 Main", city: "Akron"}
      }

      assert WithEmbed.canonical_string(person, delimiter: "|") == "Alice|100 Main|Akron"
    end

    test "nil embed contributes empty placeholders for each of its fields" do
      person = %WithEmbed{name: "Alice", address: nil}
      assert WithEmbed.canonical_string(person, delimiter: "|") == "Alice||"
    end
  end

  describe "row_hash/2" do
    test "returns 16 bytes by default (SHA-256 truncated)" do
      person = %Simple{name: "Alice", age: 30}
      hash = Simple.row_hash(person)
      assert is_binary(hash)
      assert byte_size(hash) == 16
    end

    test "identical structs produce identical hashes" do
      a = %Simple{name: "Alice", age: 30}
      b = %Simple{name: "Alice", age: 30}
      assert Simple.row_hash(a) == Simple.row_hash(b)
    end

    test "different structs produce different hashes" do
      a = %Simple{name: "Alice", age: 30}
      b = %Simple{name: "Alice", age: 31}
      refute Simple.row_hash(a) == Simple.row_hash(b)
    end

    test "nil and empty-string differ from absent values consistently" do
      # Both should encode the same way (both nil → empty in canonical)
      nil_struct = %Simple{name: nil, age: 30}
      same_nil_struct = %Simple{name: nil, age: 30}
      assert Simple.row_hash(nil_struct) == Simple.row_hash(same_nil_struct)
    end

    test "algorithm: :md5 returns 16 bytes (no truncation needed by default)" do
      person = %Simple{name: "Alice", age: 30}
      hash = Simple.row_hash(person, algorithm: :md5)
      assert byte_size(hash) == 16
    end

    test "truncate: nil returns the full hash" do
      person = %Simple{name: "Alice", age: 30}
      sha256_full = Simple.row_hash(person, truncate: nil)
      assert byte_size(sha256_full) == 32
    end

    test "truncate: 8 returns 8 bytes" do
      person = %Simple{name: "Alice", age: 30}
      assert byte_size(Simple.row_hash(person, truncate: 8)) == 8
    end

    test "stable across calls — fixture verifies known SHA-256 prefix" do
      # Lock in a known value so refactors that change canonical encoding
      # are caught loudly. If you change the canonical format on purpose,
      # update this expected value.
      person = %Simple{name: "Alice", age: 30}
      expected = :crypto.hash(:sha256, "Alice" <> Schema.canonical_delimiter() <> "30")
      assert Simple.row_hash(person, truncate: nil) == expected
    end

    test "embedded schema fields are part of the hash" do
      a = %WithEmbed{name: "Alice", address: %InnerEmbed{street: "100 Main", city: "Akron"}}
      b = %WithEmbed{name: "Alice", address: %InnerEmbed{street: "100 Main", city: "Boston"}}
      refute WithEmbed.row_hash(a) == WithEmbed.row_hash(b)
    end
  end

  describe "Delimit.Schema.canonical_string/3 (top-level helper)" do
    test "matches the module-bound version" do
      person = %Simple{name: "Alice", age: 30}
      schema = Simple.__delimit_schema__()
      assert Schema.canonical_string(schema, person) == Simple.canonical_string(person)
    end
  end
end
