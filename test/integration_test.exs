defmodule Delimit.IntegrationTest do
  use ExUnit.Case, async: true
  alias Delimit.TestSupport.Helpers
  alias Delimit.TestSupport.Schemas.FullSchema

  describe "basic end-to-end workflow" do
    test "read, modify, and write operations" do
      # Create test CSV content
      csv_string = Helpers.sample_csv_with_headers()
      
      # Read the CSV data with headers
      people = FullSchema.read_string(csv_string, headers: true)
      assert length(people) == 3
      
      # Modify the data
      modified_people = Enum.map(people, fn person ->
        %{person | first_name: String.upcase(person.first_name)}
      end)
      
      # Verify the modification
      assert Enum.at(modified_people, 0).first_name == "JOHN"
      assert Enum.at(modified_people, 1).first_name == "JANE"
      
      # Write the modified data back to a string
      output = FullSchema.write_string(modified_people, headers: true)
      
      # Verify the output contains the modified data
      assert String.contains?(output, "JOHN,Doe")
      assert String.contains?(output, "JANE,Smith")
      
      # Read the output back to ensure consistency
      reread_people = FullSchema.read_string(output, headers: true)
      assert length(reread_people) == 3
      assert Enum.at(reread_people, 0).first_name == "JOHN"
    end
    
    test "file-based workflow" do
      # Create a temporary file
      test_file = Helpers.create_temp_file(Helpers.sample_csv_with_headers())
      output_file = Helpers.temp_file_path()
      
      # Register cleanup for output file
      on_exit(fn -> File.rm(output_file) end)
      
      # Read from file
      people = FullSchema.read(test_file, headers: true)
      
      # Filter and transform the data
      transformed_people = people
        |> Enum.filter(fn person -> person.age > 25 end)
        |> Enum.map(fn person -> 
          %{person | notes: "Modified: #{person.notes || ""}"}
        end)
      
      # Write to output file
      :ok = FullSchema.write(output_file, transformed_people, headers: true)
      
      # Verify the file was created
      assert File.exists?(output_file)
      
      # Read back to verify
      result = FullSchema.read(output_file, headers: true)
      
      # Verify filtering and transformation
      assert length(result) == 3
      assert Enum.all?(result, fn person -> person.age > 25 end)
      assert Enum.all?(result, fn person -> 
        is_binary(person.notes) && String.starts_with?(person.notes, "Modified: ") 
      end)
    end
  end
  
  describe "streaming operations" do
    test "streaming with transformation" do
      # Create a temporary file
      test_file = Helpers.create_temp_file(Helpers.sample_csv_with_headers())
      output_file = Helpers.temp_file_path()
      
      # Register cleanup for output file
      on_exit(fn -> File.rm(output_file) end)
      
      # Stream from file, apply transformations, and write to output
      test_file
      |> FullSchema.stream(headers: true)
      |> Stream.map(fn person -> 
        %{person | salary: person.salary * 1.1}  # 10% salary increase
      end)
      |> FullSchema.stream_to_file(output_file, headers: true)
      
      # Read the output to verify
      result = FullSchema.read(output_file, headers: true)
      
      # Verify all records were processed and salaries were increased
      assert length(result) == 3
      
      # Compare with original data to verify the increase
      original_people = FullSchema.read(test_file, headers: true)
      
      Enum.zip(original_people, result)
      |> Enum.each(fn {original, modified} ->
        assert_in_delta modified.salary, original.salary * 1.1, 0.01
      end)
    end
  end
end