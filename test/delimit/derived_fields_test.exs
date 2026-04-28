defmodule Delimit.DerivedFieldsTest do
  use ExUnit.Case, async: true

  alias Delimit.Schema

  defmodule WithRowHash do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:age, :integer)
      field(:row_hash, :row_hash)
    end
  end

  defmodule WithRowHashOpts do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:age, :integer)
      field(:hash_md5, :row_hash, algorithm: :md5)
      field(:hash_full, :row_hash, algorithm: :sha256, truncate: nil)
    end
  end

  defmodule WithRawRow do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:age, :integer)
      field(:raw, :raw_row)
    end
  end

  defmodule WithBoth do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:age, :integer)
      field(:raw, :raw_row)
      field(:hash, :row_hash)
    end
  end

  describe ":row_hash field — basic" do
    test "populated on read with default 16-byte SHA-256 truncated hash" do
      [person] = WithRowHash.read_string("alice|30", format: :psv)
      assert person.name == "alice"
      assert person.age == 30
      assert is_binary(person.row_hash)
      assert byte_size(person.row_hash) == 16
    end

    test "two structs with same data produce same hash" do
      [a] = WithRowHash.read_string("alice|30", format: :psv)
      [b] = WithRowHash.read_string("alice|30", format: :psv)
      assert a.row_hash == b.row_hash
    end

    test "single-field difference produces different hash" do
      [a] = WithRowHash.read_string("alice|30", format: :psv)
      [b] = WithRowHash.read_string("alice|31", format: :psv)
      refute a.row_hash == b.row_hash
    end

    test "the hash matches the canonical encoding of non-derived fields only" do
      [person] = WithRowHash.read_string("alice|30", format: :psv)

      expected =
        :sha256
        |> :crypto.hash("alice" <> Schema.canonical_delimiter() <> "30")
        |> binary_part(0, 16)

      assert person.row_hash == expected
    end
  end

  describe ":row_hash field — option variants" do
    test "algorithm: :md5 produces 16 bytes (no truncation needed by default)" do
      [person] = WithRowHashOpts.read_string("alice|30", format: :psv)
      assert byte_size(person.hash_md5) == 16
    end

    test "truncate: nil returns the full hash digest" do
      [person] = WithRowHashOpts.read_string("alice|30", format: :psv)
      assert byte_size(person.hash_full) == 32
    end

    test "two row_hash fields with different algorithms produce different values" do
      [person] = WithRowHashOpts.read_string("alice|30", format: :psv)
      assert person.hash_md5 != binary_part(person.hash_full, 0, 16)
    end
  end

  describe ":row_hash — column count semantics" do
    test "input file with 2 columns produces a struct with row_hash populated; no third column needed" do
      [person] = WithRowHash.read_string("alice|30", format: :psv)
      assert person.name == "alice"
      assert person.age == 30
      assert is_binary(person.row_hash)
    end

    test "writing a struct does not emit the row_hash column" do
      person = %WithRowHash{name: "alice", age: 30, row_hash: <<1, 2, 3>>}
      output = WithRowHash.write_string([person], format: :psv)
      # 2 columns separated by | followed by a newline; no third column
      assert output == "alice|30\n"
    end

    test "headers exclude derived fields" do
      schema = WithRowHash.__delimit_schema__()
      assert Schema.headers(schema) == ["name", "age"]
    end
  end

  describe ":row_hash — round trip" do
    test "read → write → read produces the same hash" do
      [original] = WithRowHash.read_string("alice|30", format: :psv)
      written = WithRowHash.write_string([original], format: :psv)
      [reread] = WithRowHash.read_string(String.trim_trailing(written, "\n"), format: :psv)
      assert original.row_hash == reread.row_hash
    end
  end

  describe ":raw_row field" do
    test "captures all columns as strings, before type coercion" do
      [person] = WithRawRow.read_string("alice|30", format: :psv)
      assert person.raw == ["alice", "30"]
      assert person.age == 30
    end

    test "captures whitespace verbatim (before trimming applied to other fields)" do
      [person] = WithRawRow.read_string("alice|30", format: :psv)
      assert is_list(person.raw)
      assert length(person.raw) == 2
    end

    test "writing a struct does not emit the raw_row column" do
      person = %WithRawRow{name: "alice", age: 30, raw: ["alice", "30"]}
      output = WithRawRow.write_string([person], format: :psv)
      assert output == "alice|30\n"
    end
  end

  describe "multiple derived fields on same schema" do
    test "raw_row and row_hash both populated correctly" do
      [person] = WithBoth.read_string("alice|30", format: :psv)
      assert person.raw == ["alice", "30"]
      assert is_binary(person.hash)
      assert byte_size(person.hash) == 16
    end

    test "neither derived field is written" do
      person = %WithBoth{name: "alice", age: 30, raw: ["alice", "30"], hash: <<1, 2, 3>>}
      assert WithBoth.write_string([person], format: :psv) == "alice|30\n"
    end
  end

  describe "field validation" do
    test "row_hash with unknown algorithm raises" do
      assert_raise ArgumentError, ~r/algorithm must be/, fn ->
        defmodule BadAlgo do
          @moduledoc false
          use Delimit

          layout do
            field(:name, :string)
            field(:hash, :row_hash, algorithm: :sha9999)
          end
        end
      end
    end

    test "row_hash with non-positive truncate raises" do
      assert_raise ArgumentError, ~r/truncate must be a positive integer/, fn ->
        defmodule BadTrunc do
          @moduledoc false
          use Delimit

          layout do
            field(:name, :string)
            field(:hash, :row_hash, truncate: 0)
          end
        end
      end
    end

    test "row_hash with unknown option raises" do
      assert_raise ArgumentError, ~r/does not accept/, fn ->
        defmodule BadOpt do
          @moduledoc false
          use Delimit

          layout do
            field(:name, :string)
            field(:hash, :row_hash, foo: :bar)
          end
        end
      end
    end
  end

  describe "embed interaction" do
    defmodule InnerWithHash do
      @moduledoc false
      use Delimit

      layout do
        field(:street, :string)
        field(:city, :string)
        field(:inner_hash, :row_hash)
      end
    end

    defmodule OuterWithEmbed do
      @moduledoc false
      use Delimit

      layout do
        field(:name, :string)
        embeds_one(:address, InnerWithHash)
        field(:outer_hash, :row_hash)
      end
    end

    test "row_hash on outer schema includes embed contents" do
      a = %OuterWithEmbed{
        name: "alice",
        address: %InnerWithHash{street: "100 Main", city: "Akron"}
      }

      b = %OuterWithEmbed{
        name: "alice",
        address: %InnerWithHash{street: "100 Main", city: "Boston"}
      }

      assert OuterWithEmbed.row_hash(a) != OuterWithEmbed.row_hash(b)
    end
  end
end
