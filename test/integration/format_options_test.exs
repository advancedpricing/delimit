defmodule Delimit.Integration.FormatOptionsTest do
  use ExUnit.Case, async: true
  alias Delimit.TestSupport.Helpers
  alias Delimit.TestSupport.Schemas.SimpleSchema

  describe "format options" do
    test "CSV format" do
      data = [
        %SimpleSchema{name: "John Doe", age: 30, active: true},
        %SimpleSchema{name: "Jane Smith", age: 25, active: false}
      ]
      
      # Write with CSV format
      output = SimpleSchema.write_string(data, format: :csv)
      
      # Verify CSV format
      assert String.contains?(output, "John Doe,30,true")
      assert String.contains?(output, "Jane Smith,25,false")
      
      # Read back
      read_data = SimpleSchema.read_string(output, format: :csv)
      assert length(read_data) == 2
      assert Enum.at(read_data, 0).name == "John Doe"
    end
    
    test "TSV format" do
      data = [
        %SimpleSchema{name: "John Doe", age: 30, active: true},
        %SimpleSchema{name: "Jane Smith", age: 25, active: false}
      ]
      
      # Write with TSV format
      output = SimpleSchema.write_string(data, format: :tsv)
      
      # Verify TSV format
      assert String.contains?(output, "John Doe\t30\ttrue")
      assert String.contains?(output, "Jane Smith\t25\tfalse")
      
      # Read back
      read_data = SimpleSchema.read_string(output, format: :tsv)
      assert length(read_data) == 2
      assert Enum.at(read_data, 0).name == "John Doe"
    end
    
    test "PSV format" do
      data = [
        %SimpleSchema{name: "John Doe", age: 30, active: true},
        %SimpleSchema{name: "Jane Smith", age: 25, active: false}
      ]
      
      # Write with PSV format
      output = SimpleSchema.write_string(data, format: :psv)
      
      # Verify PSV format
      assert String.contains?(output, "John Doe|30|true")
      assert String.contains?(output, "Jane Smith|25|false")
      
      # Read back
      read_data = SimpleSchema.read_string(output, format: :psv)
      assert length(read_data) == 2
      assert Enum.at(read_data, 0).name == "John Doe"
    end
    
    test "custom format" do
      data = [
        %SimpleSchema{name: "John Doe", age: 30, active: true},
        %SimpleSchema{name: "Jane Smith", age: 25, active: false}
      ]
      
      # Write with custom delimiter
      output = SimpleSchema.write_string(data, delimiter: ":")
      
      # Verify custom format
      assert String.contains?(output, "John Doe:30:true")
      assert String.contains?(output, "Jane Smith:25:false")
      
      # Read back
      read_data = SimpleSchema.read_string(output, delimiter: ":")
      assert length(read_data) == 2
      assert Enum.at(read_data, 0).name == "John Doe"
    end
  end
  
  describe "format inference" do
    test "infers format from file extension" do
      data = [
        %SimpleSchema{name: "Item 1", age: 10, active: true},
        %SimpleSchema{name: "Item 2", age: 20, active: false}
      ]
      
      # Create temp files with different extensions
      csv_file = Helpers.temp_file_path() |> String.replace(".csv", ".csv")
      tsv_file = Helpers.temp_file_path() |> String.replace(".csv", ".tsv")
      psv_file = Helpers.temp_file_path() |> String.replace(".csv", ".psv")
      
      # Register cleanup
      on_exit(fn -> 
        File.rm(csv_file)
        File.rm(tsv_file)
        File.rm(psv_file)
      end)
      
      # Write to files without explicit format
      :ok = SimpleSchema.write(csv_file, data)
      :ok = SimpleSchema.write(tsv_file, data)
      :ok = SimpleSchema.write(psv_file, data)
      
      # Read content to verify format
      {:ok, csv_content} = File.read(csv_file)
      {:ok, tsv_content} = File.read(tsv_file)
      {:ok, psv_content} = File.read(psv_file)
      
      # Verify correct delimiter was used
      assert String.contains?(csv_content, "Item 1,10,true")
      assert String.contains?(tsv_content, "Item 1\t10\ttrue")
      assert String.contains?(psv_content, "Item 1|10|true")
      
      # Read back files without explicit format
      csv_data = SimpleSchema.read(csv_file)
      tsv_data = SimpleSchema.read(tsv_file)
      psv_data = SimpleSchema.read(psv_file)
      
      # Verify all formats were read correctly
      assert length(csv_data) == 2
      assert length(tsv_data) == 2
      assert length(psv_data) == 2
    end
  end
end