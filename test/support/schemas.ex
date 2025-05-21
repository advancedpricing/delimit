defmodule Delimit.TestSupport.Schemas do
  @moduledoc """
  Common test schemas for Delimit tests.

  This module contains schema definitions that are used across multiple tests.
  Using these shared schemas helps keep tests consistent and reduces duplication.
  """

  defmodule SimpleSchema do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:age, :integer)
      field(:active, :boolean)
    end
  end

  defmodule SchemaWithDefaults do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string, default: "Unknown")
      field(:age, :integer, default: 0)
      field(:active, :boolean, default: false)
    end
  end

  defmodule FullSchema do
    @moduledoc false
    use Delimit

    layout do
      field(:first_name, :string)
      field(:last_name, :string)
      field(:age, :integer)
      field(:salary, :float)
      field(:hired_date, :date)
      field(:active, :boolean)
      field(:notes, :string, nil_on_empty: true)
    end
  end

  defmodule TrimSchema do
    @moduledoc false
    use Delimit

    layout do
      field(:item, :string)
      field(:description, :string)
      field(:price, :float)
    end
  end

  defmodule CustomBoolean do
    @moduledoc false
    use Delimit

    layout do
      field(:item, :string)
      field(:paid, :boolean, true_values: ["paid"], false_values: ["billed", "pending"])
    end
  end

  defmodule Address do
    @moduledoc false
    use Delimit

    layout do
      field(:street, :string)
      field(:city, :string)
      field(:state, :string, nil_on_empty: true)
      field(:postal_code, :string)
    end
  end

  defmodule Customer do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)
      field(:contact_email, :string)
      embeds_one(:billing_address, Address)
      embeds_one(:shipping_address, Address)
    end
  end

  # Helper module for custom conversions
  defmodule CustomConversionHelpers do
    @moduledoc false
    def split_tags(val), do: String.split(val, "|")
    def join_tags(val), do: Enum.join(val, "|")
  end

  defmodule CustomConversion do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string)

      field(:tags, :string,
        read_fn: &CustomConversionHelpers.split_tags/1,
        write_fn: &CustomConversionHelpers.join_tags/1
      )
    end
  end
end
