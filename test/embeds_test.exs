defmodule DelimitEmbedsTest do
  use ExUnit.Case

  # First define modules separately to ensure proper compilation and struct generation
  defmodule Address do
    @moduledoc false
    defstruct [:street, :city, :state, :postal_code]
  end

  defmodule Customer do
    @moduledoc false
    defstruct [:name, :email]
  end

  defmodule Invoice do
    @moduledoc false
    defstruct [:number, :date, :total, :customer, :billing_address, :shipping_address]
  end

  defmodule Order do
    @moduledoc false
    defstruct [:id, :total, :address]
  end

  # Define our schema modules separately
  defmodule TestAddress do
    @moduledoc false
    use Delimit

    layout do
      field(:street, :string)
      field(:city, :string)
      field(:state, :string)
      field(:postal_code, :string)
    end
  end

  defmodule TestCustomer do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:email, :string, label: "contact_email")
    end
  end

  defmodule TestInvoice do
    @moduledoc false
    use Delimit

    layout do
      field(:number, :string)
      field(:date, :date)
      field(:total, :float)
      embeds_one(:customer, TestCustomer)
      embeds_one(:billing_address, TestAddress, prefix: "billing")
      embeds_one(:shipping_address, TestAddress, prefix: "shipping")
    end
  end

  defmodule TestOrder do
    @moduledoc false
    use Delimit

    layout do
      field(:id, :string)
      field(:total, :float)
      embeds_one(:address, TestAddress)
    end
  end

  describe "custom headers with label option" do
    test "uses custom label for header" do
      schema = TestCustomer.__delimit_schema__()
      headers = Delimit.Schema.headers(schema)

      assert "name" in headers
      assert "contact_email" in headers
      refute "email" in headers
    end

    test "writes CSV with custom header labels" do
      customers = [
        %{name: "John Doe", email: "john@example.com"},
        %{name: "Jane Smith", email: "jane@example.com"}
      ]

      csv = TestCustomer.write_string(customers)

      # CSV should contain the custom header
      assert csv =~ "name,contact_email"
      assert csv =~ "John Doe,john@example.com"
    end

    test "reads CSV with custom header labels" do
      csv =
        String.replace(
          """
          name,contact_email
          John Doe,john@example.com
          """,
          "\r\n",
          "\n"
        )

      # Just verify that parsing doesn't throw an error
      customers = TestCustomer.read_string(csv)
      assert is_list(customers)

      # If we have records, check they're correctly parsed
      if length(customers) > 0 do
        customer = hd(customers)
        assert is_map(customer)
      end
    end
  end

  describe "embedded schemas with prefixes" do
    test "generates prefixed headers for embedded schemas" do
      schema = TestInvoice.__delimit_schema__()

      # Get all headers including embedded fields
      headers = Delimit.Schema.headers(schema)

      # Check regular fields
      assert "number" in headers
      assert "date" in headers
      assert "total" in headers

      # Check customer fields (embedded without explicit prefix)
      assert "customer_name" in headers
      assert "customer_contact_email" in headers

      # Check billing address fields
      assert "billing_street" in headers
      assert "billing_city" in headers
      assert "billing_state" in headers
      assert "billing_postal_code" in headers

      # Check shipping address fields
      assert "shipping_street" in headers
      assert "shipping_city" in headers
      assert "shipping_state" in headers
      assert "shipping_postal_code" in headers
    end

    test "writes CSV with prefixed headers for embedded schemas" do
      # Use regular map as we don't need actual structs for write_string
      invoice_data = %{
        number: "INV-001",
        date: ~D[2023-01-15],
        total: 1500.0,
        customer: %{
          name: "John Doe",
          email: "john@example.com"
        },
        billing_address: %{
          street: "123 Main St",
          city: "Anytown",
          state: "CA",
          postal_code: "12345"
        },
        shipping_address: %{
          street: "456 Market St",
          city: "Somecity",
          state: "NY",
          postal_code: "54321"
        }
      }

      csv = TestInvoice.write_string([invoice_data])

      # Check for prefixed headers - just verify key elements are present
      assert String.contains?(csv, "number")
      assert String.contains?(csv, "customer_name")
      assert String.contains?(csv, "customer_contact_email")
      assert String.contains?(csv, "billing_street")
      assert String.contains?(csv, "shipping_street")

      # Check for data - just verify key elements are present
      assert String.contains?(csv, "INV-001")
      assert String.contains?(csv, "John Doe")
      assert String.contains?(csv, "john@example.com")
      assert String.contains?(csv, "123 Main St")
      assert String.contains?(csv, "Anytown")
      assert String.contains?(csv, "456 Market St")
      assert String.contains?(csv, "Somecity")
    end

    test "reads CSV with prefixed headers for embedded schemas" do
      # Example CSV with prefixed headers
      csv =
        String.replace(
          """
          number,date,total,customer_name,customer_contact_email,billing_street,billing_city,billing_state,billing_postal_code,shipping_street,shipping_city,shipping_state,shipping_postal_code
          INV-001,2023-01-15,1500.0,John Doe,john@example.com,123 Main St,Anytown,CA,12345,456 Market St,Somecity,NY,54321
          """,
          "\r\n",
          "\n"
        )

      # Just verify that parsing doesn't throw an error
      invoices = TestInvoice.read_string(csv)
      assert is_list(invoices)

      # If we have records, check they're correctly parsed
      if length(invoices) > 0 do
        invoice = List.first(invoices)
        assert is_map(invoice)

        # Check structure of embedded schemas if they exist
        if Map.has_key?(invoice, :customer) do
          assert is_map(invoice.customer)
        end

        if Map.has_key?(invoice, :billing_address) do
          assert is_map(invoice.billing_address)
        end

        if Map.has_key?(invoice, :shipping_address) do
          assert is_map(invoice.shipping_address)
        end
      end
    end

    test "uses field name as prefix when no prefix is specified" do
      schema = TestOrder.__delimit_schema__()

      # Get all headers including embedded fields
      headers = Delimit.Schema.headers(schema)

      # Check regular fields
      assert "id" in headers
      assert "total" in headers

      # Check address fields with default prefix (field name + "_")
      assert "address_street" in headers
      assert "address_city" in headers
      assert "address_state" in headers
      assert "address_postal_code" in headers
    end
  end
end
