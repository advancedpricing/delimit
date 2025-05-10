defmodule PerformanceBenchmark do
  @moduledoc """
  Enhanced benchmarks comparing optimized Delimit with NimbleCSV for CSV parsing.
  
  This benchmark focuses on comparing:
  1. NimbleCSV with manual type conversions
  2. Optimized Delimit (schema-based with automatic type conversions)
  
  We'll test with small, medium, and large datasets to see how the optimizations
  impact performance with different data sizes.
  """
  
  # Define test data sizes
  @small_rows 1_000
  @medium_rows 10_000
  @large_rows 50_000
  @temp_file_prefix "bench/perf_data_"
  
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
  NimbleCSV.define(PerformanceBenchmark.Parser, separator: ",")
  
  def setup do
    # Create directory if not exists
    File.mkdir_p("bench")
    
    # Generate data for each size and save to file
    inputs = %{
      "small" => generate_dataset(@small_rows),
      "medium" => generate_dataset(@medium_rows),
      "large" => generate_dataset(@large_rows)
    }
    
    inputs
  end
  
  def cleanup do
    # Delete all temporary files
    ["small", "medium", "large"]
    |> Enum.each(fn size ->
      File.rm("#{@temp_file_prefix}#{size}.csv")
    end)
  end
  
  def generate_dataset(rows) do
    # Generate CSV data
    csv_data = generate_csv_data(rows)
    
    # Save to file
    file_path = "#{@temp_file_prefix}#{rows}.csv"
    File.write!(file_path, csv_data)
    
    # Return the input info
    %{csv_data: csv_data, file_path: file_path, row_count: rows}
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
  
  def run_benchmarks(inputs) do
    Benchee.run(
      %{
        "NimbleCSV + type conversions" => fn %{csv_data: csv_data} ->
          PerformanceBenchmark.Parser.parse_string(csv_data)
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
        
        "Optimized Delimit (schema-based)" => fn %{csv_data: csv_data} ->
          TestPerson.read_string(csv_data)
        end,
        
        "NimbleCSV from file + conversions" => fn %{file_path: file_path} ->
          file_path
          |> File.stream!()
          |> PerformanceBenchmark.Parser.parse_stream()
          |> Stream.map(fn [id, first_name, last_name, age, salary, hired_date, active, score] ->
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
          |> Enum.to_list()
        end,
        
        "Optimized Delimit from file" => fn %{file_path: file_path} ->
          TestPerson.read(file_path)
        end
      },
      inputs: inputs,
      time: 10,
      memory_time: 2,
      print: [
        benchmarking: true,
        configuration: true,
        fast_warning: true
      ],
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: "bench/output/performance.html", auto_open: false}
      ]
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

# Create output directory for HTML reports
File.mkdir_p("bench/output")

# Run the benchmark
IO.puts("Setting up benchmarks...")
inputs = PerformanceBenchmark.setup()

try do
  IO.puts("Running benchmarks comparing NimbleCSV vs Optimized Delimit...")
  PerformanceBenchmark.run_benchmarks(inputs)
after
  IO.puts("Cleaning up temporary files...")
  PerformanceBenchmark.cleanup()
end