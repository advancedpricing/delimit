defmodule Delimit.EmbedsTest do
  use ExUnit.Case, async: true
  alias Delimit.TestSupport.Schemas.{Address, Customer}

  describe "embedded schema functionality" do
    test "defines schema with embedded fields" do
      schema = Customer.__delimit_schema__()
      
      # Check that the schema contains the embedded fields
      assert length(schema.fields) == 4
      
      # Check that embeds are properly registered
      assert Map.has_key?(schema.embeds, :billing_address)
      assert Map.has_key?(schema.embeds, :shipping_address)
      
      # Check that the embedded modules are correct
      assert schema.embeds.billing_address == Address
      assert schema.embeds.shipping_address == Address
    end
    
    test "reads data with embedded fields" do
      # CSV with headers for main and embedded fields
      csv_string = """
      name,contact_email,billing_address_street,billing_address_city,billing_address_state,billing_address_postal_code,shipping_address_street,shipping_address_city,shipping_address_state,shipping_address_postal_code
      ACME Corp,contact@acme.com,123 Billing St,Billing City,CA,90001,456 Shipping Rd,Shipping City,NY,10001
      """
      
      customers = Customer.read_string(csv_string, headers: true)
      assert length(customers) == 1
      
      # Verify the main fields were read correctly
      customer = Enum.at(customers, 0)
      assert customer.name == "ACME Corp"
      assert customer.contact_email == "contact@acme.com"
      
      # Verify the embedded billing address was read correctly
      assert customer.billing_address.street == "123 Billing St"
      assert customer.billing_address.city == "Billing City"
      assert customer.billing_address.state == "CA"
      assert customer.billing_address.postal_code == "90001"
      
      # Verify the embedded shipping address was read correctly
      assert customer.shipping_address.street == "456 Shipping Rd"
      assert customer.shipping_address.city == "Shipping City"
      assert customer.shipping_address.state == "NY"
      assert customer.shipping_address.postal_code == "10001"
    end
    
    test "writes data with embedded fields" do
      # Create a customer with embedded addresses
      customer = struct(Customer, %{
        name: "ACME Corp",
        contact_email: "contact@acme.com",
        billing_address: struct(Address, %{
          street: "123 Billing St",
          city: "Billing City",
          state: "CA",
          postal_code: "90001"
        }),
        shipping_address: struct(Address, %{
          street: "456 Shipping Rd",
          city: "Shipping City",
          state: "NY",
          postal_code: "10001"
        })
      })
      
      # Write to string with headers
      output = Customer.write_string([customer], headers: true)
      

      
      # Verify output contains all fields
      assert String.contains?(output, "name,contact_email,billing_address_street")
      assert String.contains?(output, "ACME Corp,contact@acme.com,123 Billing St")
      assert String.contains?(output, "Billing City,CA,90001")
      assert String.contains?(output, "456 Shipping Rd,Shipping City,NY,10001")
    end
    
    test "round-trip with embedded fields" do
      # Create original customer data
      original_customer = struct(Customer, %{
        name: "ACME Corp",
        contact_email: "contact@acme.com",
        billing_address: struct(Address, %{
          street: "123 Billing St",
          city: "Billing City",
          state: "CA",
          postal_code: "90001"
        }),
        shipping_address: struct(Address, %{
          street: "456 Shipping Rd",
          city: "Shipping City",
          state: "NY",
          postal_code: "10001"
        })
      })
      
      # Write to string with headers so embedded fields are correctly mapped
      output = Customer.write_string([original_customer], headers: true)
      
      # Read back with headers enabled
      customers = Customer.read_string(output, headers: true)
      
      # Verify data is preserved
      assert length(customers) == 1
      customer = Enum.at(customers, 0)
      
      # Check main fields
      assert customer.name == "ACME Corp"
      assert customer.contact_email == "contact@acme.com"
      
      # Check billing address
      assert customer.billing_address.street == "123 Billing St"
      assert customer.billing_address.city == "Billing City"
      assert customer.billing_address.state == "CA"
      assert customer.billing_address.postal_code == "90001"
      
      # Check shipping address
      assert customer.shipping_address.street == "456 Shipping Rd"
      assert customer.shipping_address.city == "Shipping City"
      assert customer.shipping_address.state == "NY"
      assert customer.shipping_address.postal_code == "10001"
    end
    
    test "handles partial embedded data" do
      # CSV with some missing embedded fields
      csv_string = """
      name,contact_email,billing_address_street,billing_address_city,shipping_address_street
      ACME Corp,contact@acme.com,123 Billing St,Billing City,456 Shipping Rd
      """
      
      customers = Customer.read_string(csv_string, headers: true)
      assert length(customers) == 1
      
      customer = Enum.at(customers, 0)
      
      # Check billing address - partially filled
      assert customer.billing_address.street == "123 Billing St"
      assert customer.billing_address.city == "Billing City"
      assert customer.billing_address.state == nil
      assert customer.billing_address.postal_code == nil
      
      # Check shipping address - mostly empty
      assert customer.shipping_address.street == "456 Shipping Rd"
      assert customer.shipping_address.city == nil
      assert customer.shipping_address.state == nil
      assert customer.shipping_address.postal_code == nil
    end
  end
end