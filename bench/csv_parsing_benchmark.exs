defmodule DelimitBench do
  @moduledoc """
  Benchmarks for comparing Delimit with NimbleCSV for CSV parsing.
  
  This benchmark compares three approaches:
  1. Parsing with NimbleCSV (raw parsing, no type conversions)
  2. Parsing with NimbleCSV + manual type conversions
  3. Parsing with Delimit (schema-based with automatic type conversions)
  """
  
  # Define the CSV input size for benchmarking
  @rows 10_000
  @temp_file "bench/temp_data.csv"
  
  # Define a test schema for Delimit
  defmodule TestPerson do
    @moduledoc false
    use Delimit
    
    layout do
      field(:id, :integer)
      field(:first_name, :string)
      field(:last_name, :string)
      field(:age, :integer)
      field(:salary, :float)
      field(:hired_date, :date)
      field(:active, :boolean)
      field(:score, :float)
    end
  end
  
  # Define a NimbleCSV parser
  NimbleCSV.define(DelimitBench.Parser, separator: ",")
  
  def setup do
    # Create test data
    csv_data = generate_csv_data(@rows)
    
    # Create directory if not exists
    File.mkdir_p("bench")
    
    # Write the test data to a file
    File.write!(@temp_file, csv_data)
    
    # Return the csv data as string for string-based benchmarks
    csv_data
  end
  
  def cleanup do
    # Delete the temporary file
    File.rm(@temp_file)
  end
  
  def generate_csv_data(rows) do
    header = "id,first_name,last_name,age,salary,hired_date,active,score\n"
    
    rows_data =
      1..rows
      |> Enum.map(fn i ->
        first_name = "First#{i}"
        last_name = "Last#{rem(i, 100)}"
        age = 20 + rem(i, 50)
        salary = 50_000.00 + i / 10
        hired_date = "2020-#{String.pad_leading("#{1 + rem(i, 12)}", 2, "0")}-#{String.pad_leading("#{1 + rem(i, 28)}", 2, "0")}"
        active = if rem(i, 2) == 0, do: "true", else: "false"
        score = i / 100
        
        "#{i},#{first_name},#{last_name},#{age},#{salary},#{hired_date},#{active},#{score}"
      end)
      |> Enum.join("\n")
    
    header <> rows_data
  end
  
  def run_benchmarks(csv_data) do
    Benchee.run(
      %{
        "NimbleCSV (raw parsing)" => fn ->
          DelimitBench.Parser.parse_string(csv_data)
        end,
        
        "NimbleCSV + type conversions" => fn ->
          DelimitBench.Parser.parse_string(csv_data)
          |> Enum.map(fn [id, first_name, last_name, age, salary, hired_date, active, score] ->
            %{
              id: parse_integer(id),
              first_name: first_name,
              last_name: last_name,
              age: parse_integer(age),
              salary: parse_float(salary),
              hired_date: parse_date(hired_date),
              active: parse_boolean(active),
              score: parse_float(score)
            }
          end)
        end,
        
        "Delimit (schema-based)" => fn ->
          TestPerson.read_string(csv_data)
        end,
        
        "NimbleCSV from file (raw)" => fn ->
          @temp_file
          |> File.stream!()
          |> DelimitBench.Parser.parse_stream()
          |> Enum.to_list()
        end,
        
        "Delimit from file (schema-based)" => fn ->
          TestPerson.read(@temp_file)
        end
      },
      time: 10,
      memory_time: 2
    )
  end
  
  # Helper functions for manual type conversion
  defp parse_integer(value) do
    case Integer.parse(value) do
      {int, _} -> int
      _ -> nil
    end
  end
  
  defp parse_float(value) do
    case Float.parse(value) do
      {float, _} -> float
      _ -> nil
    end
  end
  
  defp parse_date(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      _ -> nil
    end
  end
  
  defp parse_boolean("true"), do: true
  defp parse_boolean("yes"), do: true
  defp parse_boolean("1"), do: true
  defp parse_boolean(_), do: false
end

# Run the benchmark
csv_data = DelimitBench.setup()

try do
  DelimitBench.run_benchmarks(csv_data)
after
  DelimitBench.cleanup()
end