defmodule ScalingBench do
  @moduledoc """
  Benchmarks comparing Delimit vs NimbleCSV performance at different data sizes.
  
  This benchmark shows how each parsing method scales with increasing data size.
  We'll test with 100, 1,000, 10,000, and 50,000 rows to see scaling characteristics.
  """
  
  @data_sizes [100, 1_000, 10_000, 50_000]
  @temp_file_prefix "bench/scaling_data_"
  
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
  NimbleCSV.define(ScalingBench.Parser, separator: ",")
  
  def setup do
    # Create directory if not exists
    File.mkdir_p("bench")
    
    # Generate data for each size and save to file
    inputs =
      @data_sizes
      |> Map.new(fn size ->
        # Generate CSV data
        csv_data = generate_csv_data(size)
        
        # Save to file
        file_path = "#{@temp_file_prefix}#{size}.csv"
        File.write!(file_path, csv_data)
        
        # Return the input 
        {size, %{csv_data: csv_data, file_path: file_path}}
      end)
    
    inputs
  end
  
  def cleanup do
    # Delete all temporary files
    @data_sizes
    |> Enum.each(fn size ->
      File.rm("#{@temp_file_prefix}#{size}.csv")
    end)
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
        "NimbleCSV (raw parsing)" => fn %{csv_data: csv_data} ->
          ScalingBench.Parser.parse_string(csv_data)
        end,
        
        "NimbleCSV + type conversions" => fn %{csv_data: csv_data} ->
          ScalingBench.Parser.parse_string(csv_data)
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
        
        "Delimit (schema-based)" => fn %{csv_data: csv_data} ->
          TestPerson.read_string(csv_data)
        end,
        
        "NimbleCSV from file (raw)" => fn %{file_path: file_path} ->
          file_path
          |> File.stream!()
          |> ScalingBench.Parser.parse_stream()
          |> Enum.to_list()
        end,
        
        "Delimit from file (schema-based)" => fn %{file_path: file_path} ->
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
        {Benchee.Formatters.HTML, file: "bench/output/scaling.html", auto_open: false}
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
inputs = ScalingBench.setup()

try do
  ScalingBench.run_benchmarks(inputs)
after
  ScalingBench.cleanup()
end