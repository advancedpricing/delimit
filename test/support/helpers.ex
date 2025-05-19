defmodule Delimit.TestSupport.Helpers do
  @moduledoc """
  Common helper functions for Delimit tests.

  This module provides reusable functionality for test cases across the suite.
  """

  @doc """
  Creates a temporary CSV file with the given content.

  Returns the path to the created file. The file will be automatically
  deleted when the test process exits.

  ## Example

      test_file = Delimit.TestSupport.Helpers.create_temp_file("name,age\\nJohn,30\\n")
  """
  @spec create_temp_file(binary()) :: String.t()
  def create_temp_file(content) do
    test_file = temp_file_path()
    File.write!(test_file, content)

    # Register cleanup when test exits
    ExUnit.Callbacks.on_exit(fn ->
      File.rm(test_file)
    end)

    test_file
  end

  @doc """
  Generates a path for a temporary file with a random name.
  """
  @spec temp_file_path() :: String.t()
  def temp_file_path do
    Path.join(System.tmp_dir(), "delimit_test_#{:rand.uniform(1_000_000)}.csv")
  end

  @doc """
  Creates sample data for a person schema.

  ## Example

      people = Delimit.TestSupport.Helpers.sample_people(3)
  """
  @spec sample_people(non_neg_integer()) :: [map()]
  def sample_people(count \\ 2) do
    for i <- 1..count do
      %{
        first_name: "First#{i}",
        last_name: "Last#{i}",
        age: 20 + rem(i, 50),
        salary: 30_000.0 + i * 100,
        hired_date: ~D[2020-01-15],
        active: rem(i, 2) == 0,
        notes: if(rem(i, 3) == 0, do: "Note for #{i}")
      }
    end
  end

  @doc """
  Creates a CSV string with headers and sample data.
  """
  @spec sample_csv_with_headers() :: String.t()
  def sample_csv_with_headers do
    "first_name,last_name,age,salary,hired_date,active,notes\r\n" <>
      "John,Doe,30,50000.50,2020-01-15,true,Good employee\r\n" <>
      "Jane,Smith,28,55000.75,2019-05-20,true,\r\n" <>
      "Bob,Johnson,45,75000.00,2015-11-10,false,On probation"
  end

  @doc """
  Creates a CSV string with comments and headers before data.
  """
  @spec sample_csv_with_comments() :: String.t()
  def sample_csv_with_comments do
    "# This is a comment\r\n" <>
      "# Another comment line\r\n" <>
      "first_name,last_name,age,salary,hired_date,active,notes\r\n" <>
      "John,Doe,30,50000.50,2020-01-15,true,Good employee\r\n" <>
      "Jane,Smith,28,55000.75,2019-05-20,true,\r\n" <>
      "Bob,Johnson,45,75000.00,2015-11-10,false,On probation"
  end
end
