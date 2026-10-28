defmodule Delimit.HeaderValidationTest do
  use ExUnit.Case, async: true

  alias Delimit.HeaderError

  @moduletag :tmp_dir

  defmodule Payment do
    @moduledoc false
    use Delimit

    layout do
      field(:case_number, :string, label: "CASE ID")
      field(:amount, :string, label: "AMOUNT")
    end
  end

  describe "validate_headers: true" do
    test "reads a file whose header matches", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, "CASE ID,AMOUNT\n999,10.00\n")

      assert [%Payment{case_number: "999"}] =
               Payment.read(path, headers: true, validate_headers: true)

      assert [%Payment{case_number: "999"}] =
               path |> Payment.stream(headers: true, validate_headers: true) |> Enum.to_list()
    end

    test "ignores case, surrounding spaces, a byte order mark and CRLF", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, "\uFEFFcase id , Amount\r\n999,10.00\r\n")

      assert [%Payment{case_number: "999", amount: "10.00"}] =
               Payment.read(path, headers: true, validate_headers: true)

      assert [%Payment{case_number: "999", amount: "10.00"}] =
               path |> Payment.stream(headers: true, validate_headers: true) |> Enum.to_list()
    end

    test "raises on columns in the wrong order", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, "AMOUNT,CASE ID\n10.00,999\n")

      for read <- readers() do
        error = assert_raise HeaderError, fn -> read.(path) end

        assert error.found == ["AMOUNT", "CASE ID"]
        assert error.missing == []
        assert error.unexpected == []

        assert Exception.message(error) ==
                 "expected the columns CASE ID, AMOUNT but found the columns AMOUNT, CASE ID"
      end
    end

    test "names missing and unexpected columns", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, "\uFEFFCASE,AMOUNT,MEMO\n999,10.00,x\n")

      for read <- readers() do
        error = assert_raise HeaderError, fn -> read.(path) end

        assert error.missing == ["CASE ID"]
        assert error.unexpected == ["CASE", "MEMO"]

        assert Exception.message(error) ==
                 "expected the columns CASE ID, AMOUNT but found the columns CASE, AMOUNT, MEMO " <>
                   "(missing: CASE ID; unexpected: CASE, MEMO)"
      end
    end

    test "raises on an empty file", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, "")

      for read <- readers() do
        error = assert_raise HeaderError, fn -> read.(path) end
        assert error.found == []
        assert Exception.message(error) =~ "but found no header"
      end
    end

    test "checks the header that :skip_while leaves first", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, "# exported\nCASE ID,AMOUNT\n999,10.00\n")
      opts = [headers: true, validate_headers: true, skip_while: &String.starts_with?(&1, "#")]

      assert [%Payment{case_number: "999"}] = Payment.read(path, opts)
      assert [%Payment{case_number: "999"}] = path |> Payment.stream(opts) |> Enum.to_list()
    end

    test "stream/2 raises when called, before any row is read", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, "AMOUNT\n10.00\n")

      assert_raise HeaderError, fn ->
        Payment.stream(path, headers: true, validate_headers: true)
      end
    end

    test "requires headers: true", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, "CASE ID,AMOUNT\n999,10.00\n")

      for read <- [&Payment.read/2, &Payment.stream/2] do
        assert_raise ArgumentError, ~r/requires headers: true/, fn ->
          read.(path, validate_headers: true)
        end
      end
    end
  end

  describe "without validate_headers" do
    test "a mismatched header is still read by position", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, "AMOUNT,CASE ID\n10.00,999\n")

      assert [%Payment{case_number: "10.00"}] =
               path |> Payment.stream(headers: true) |> Enum.to_list()
    end
  end

  describe "HeaderError.check/2" do
    test "returns :ok for matching headers" do
      assert HeaderError.check(["a", "b"], [" A", "b "]) == :ok
    end

    test "returns the error for headers that do not match" do
      assert {:error, %HeaderError{missing: ["b"], unexpected: []}} =
               HeaderError.check(["a", "b"], ["a"])
    end
  end

  defp readers do
    [
      &Payment.read(&1, headers: true, validate_headers: true),
      &(&1 |> Payment.stream(headers: true, validate_headers: true) |> Enum.to_list())
    ]
  end

  defp write(tmp_dir, content) do
    path = Path.join(tmp_dir, "payments_#{System.unique_integer([:positive])}.csv")
    File.write!(path, content)
    path
  end
end
