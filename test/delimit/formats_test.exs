defmodule Delimit.FormatsTest do
  use ExUnit.Case, async: true

  alias Delimit.Formats

  describe "get_options/1" do
    test "returns options for CSV format" do
      options = Formats.get_options(:csv)
      assert options[:delimiter] == ","
      assert options[:escape] == "\""
      assert options[:headers] == true
    end

    test "returns options for TSV format" do
      options = Formats.get_options(:tsv)
      assert options[:delimiter] == "\t"
      assert options[:escape] == "\""
      assert options[:headers] == true
    end

    test "returns options for PSV format" do
      options = Formats.get_options(:psv)
      assert options[:delimiter] == "|"
      assert options[:escape] == "\""
      assert options[:headers] == true
    end

    test "returns options for SSV format" do
      options = Formats.get_options(:ssv)
      assert options[:delimiter] == ";"
      assert options[:escape] == "\""
      assert options[:headers] == true
    end

    test "raises for unsupported format" do
      assert_raise ArgumentError, "Unsupported format: :invalid", fn ->
        Formats.get_options(:invalid)
      end
    end
  end

  describe "supported_formats/0" do
    test "returns list of supported formats" do
      formats = Formats.supported_formats()
      assert Enum.sort(formats) == Enum.sort([:csv, :tsv, :psv, :ssv])
    end
  end

  describe "merge_options/3" do
    test "merges schema options with format options and custom options" do
      schema_opts = [headers: false, trim_fields: true]
      format_opts = :csv
      custom_opts = [escape: "'", headers: true]

      merged = Formats.merge_options(schema_opts, format_opts, custom_opts)

      assert merged[:headers] == true # from custom_opts
      assert merged[:trim_fields] == true # from schema_opts
      assert merged[:delimiter] == "," # from format_opts
      assert merged[:escape] == "'" # from custom_opts
    end

    test "handles nil format" do
      schema_opts = [headers: false, trim_fields: true]
      custom_opts = [escape: "'", headers: true]

      merged = Formats.merge_options(schema_opts, nil, custom_opts)

      assert merged[:headers] == true # from custom_opts
      assert merged[:trim_fields] == true # from schema_opts
      assert merged[:escape] == "'" # from custom_opts
      refute Keyword.has_key?(merged, :delimiter) # not added because no format
    end
  end
end