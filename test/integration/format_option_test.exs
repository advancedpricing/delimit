defmodule Delimit.Integration.FormatOptionTest do
  use ExUnit.Case, async: true
  
  defmodule Person do
    use Delimit
    
    layout do
      field :first_name, :string
      field :last_name, :string
      field :age, :integer
    end
  end
  
  setup do
    # Create temporary directory for test files
    tmp_dir = System.tmp_dir!()
    test_dir = Path.join(tmp_dir, "delimit_format_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(test_dir)
    
    # Return test directory path
    %{test_dir: test_dir}
  end
  
  test "write_string with format option", %{test_dir: _test_dir} do
    people = [
      %Person{first_name: "Write", last_name: "String", age: 42}
    ]
    
    # Test with different formats
    csv_string = Person.write_string(people, format: :csv)
    tsv_string = Person.write_string(people, format: :tsv)
    psv_string = Person.write_string(people, format: :psv)
    ssv_string = Person.write_string(people, format: :ssv)
    
    # Verify correct separators in output strings
    assert csv_string =~ "Write,String,42"
    assert tsv_string =~ "Write\tString\t42"
    assert psv_string =~ "Write|String|42"
    assert ssv_string =~ "Write;String;42"
  end
  
  test "format option with explicit struct creation", %{test_dir: test_dir} do
    # Create test files with specific content
    csv_content = """
    first_name,last_name,age
    John,Doe,30
    """
    
    tsv_content = """
    first_name\tlast_name\tage
    Bob\tJones\t45
    """
    
    psv_content = """
    first_name|last_name|age
    Maria|Lopez|55
    """
    
    ssv_content = """
    first_name;last_name;age
    Carlos;Garcia;40
    """
    
    # Write test files
    csv_path = Path.join(test_dir, "manual.csv")
    tsv_path = Path.join(test_dir, "manual.tsv")
    psv_path = Path.join(test_dir, "manual.psv")
    ssv_path = Path.join(test_dir, "manual.ssv")
    
    File.write!(csv_path, csv_content)
    File.write!(tsv_path, tsv_content)
    File.write!(psv_path, psv_content)
    File.write!(ssv_path, ssv_content)
    
    # Test write_string output matches expected format patterns
    assert Person.write_string([%Person{first_name: "Test", last_name: "User", age: 25}], format: :csv) =~ "Test,User,25"
    assert Person.write_string([%Person{first_name: "Test", last_name: "User", age: 25}], format: :tsv) =~ "Test\tUser\t25"
    assert Person.write_string([%Person{first_name: "Test", last_name: "User", age: 25}], format: :psv) =~ "Test|User|25"
    assert Person.write_string([%Person{first_name: "Test", last_name: "User", age: 25}], format: :ssv) =~ "Test;User;25"
  end
  
  test "format option for CSV writing", %{test_dir: test_dir} do
    # Create data directly with Person struct
    people = [
      %Person{first_name: "John", last_name: "Doe", age: 30},
      %Person{first_name: "Jane", last_name: "Smith", age: 28}
    ]
    
    # Write with format option
    csv_path = Path.join(test_dir, "people.csv")
    :ok = Person.write(csv_path, people, format: :csv)
    
    # Verify file content directly
    content = File.read!(csv_path)
    assert content =~ "first_name,last_name,age"
    assert content =~ "John,Doe,30"
    assert content =~ "Jane,Smith,28"
    
    # Content shouldn't contain tab or pipe delimiters
    refute String.contains?(content, "\t")
    refute String.contains?(content, "|")
  end
  
  test "format option for TSV writing", %{test_dir: test_dir} do
    # Create data directly with Person struct
    people = [
      %Person{first_name: "Bob", last_name: "Jones", age: 45},
      %Person{first_name: "Alice", last_name: "Brown", age: 38}
    ]
    
    # Write with format option
    tsv_path = Path.join(test_dir, "people.tsv")
    :ok = Person.write(tsv_path, people, format: :tsv)
    
    # Verify file content directly
    content = File.read!(tsv_path)
    assert content =~ "first_name\tlast_name\tage"
    assert content =~ "Bob\tJones\t45"
    assert content =~ "Alice\tBrown\t38"
    
    # Content shouldn't contain comma or pipe delimiters
    refute String.contains?(content, ",")
    refute String.contains?(content, "|")
  end
  
  test "explicit delimiter overrides format", %{test_dir: test_dir} do
    # Create data directly with Person struct
    people = [
      %Person{first_name: "Sam", last_name: "Wilson", age: 33}
    ]
    
    # Write with semicolon delimiter - should override format's comma delimiter
    semi_path = Path.join(test_dir, "people.txt")
    :ok = Person.write(semi_path, people, format: :csv, delimiter: ";")
    
    # Verify file content directly
    content = File.read!(semi_path)
    assert content =~ "first_name;last_name;age"
    assert content =~ "Sam;Wilson;33"
    
    # Content shouldn't contain comma or tab delimiters
    refute String.contains?(content, ",")
    refute String.contains?(content, "\t")
  end
  
  test "format option for SSV writing", %{test_dir: test_dir} do
    # Create data directly with Person struct
    people = [
      %Person{first_name: "Carlos", last_name: "Garcia", age: 40},
      %Person{first_name: "Elena", last_name: "Martinez", age: 36}
    ]
    
    # Write with format option
    ssv_path = Path.join(test_dir, "people.ssv")
    :ok = Person.write(ssv_path, people, format: :ssv)
    
    # Verify file content directly
    content = File.read!(ssv_path)
    assert content =~ "first_name;last_name;age"
    assert content =~ "Carlos;Garcia;40"
    assert content =~ "Elena;Martinez;36"
    
    # Content shouldn't contain comma, tab or pipe delimiters
    refute String.contains?(content, ",")
    refute String.contains?(content, "\t")
    refute String.contains?(content, "|")
  end
end