defmodule Delimit.TestSupport.FixedWidthSchemas do
  @moduledoc false

  defmodule SimpleFixed do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string, width: 10)
      field(:age, :integer, width: 5)
      field(:active, :boolean, width: 5)
    end
  end

  defmodule FullFixed do
    @moduledoc false
    use Delimit

    layout do
      field(:first_name, :string, width: 15)
      field(:last_name, :string, width: 15)
      field(:age, :integer, width: 5)
      field(:salary, :float, width: 12)
      field(:hired_date, :date, width: 10)
      field(:active, :boolean, width: 5)
    end
  end

  defmodule RightJustified do
    @moduledoc false
    use Delimit

    layout do
      field(:id, :integer, width: 10, justify: :right)
      field(:name, :string, width: 20)
      field(:amount, :float, width: 12, justify: :right)
    end
  end

  defmodule ZeroPadded do
    @moduledoc false
    use Delimit

    layout do
      field(:id, :integer, width: 8, justify: :right, pad_char: "0")
      field(:code, :string, width: 5, justify: :right, pad_char: "0")
    end
  end

  defmodule FixedAddress do
    @moduledoc false
    use Delimit

    layout do
      field(:street, :string, width: 20)
      field(:city, :string, width: 15)
      field(:state, :string, width: 2)
      field(:zip, :string, width: 5)
    end
  end

  defmodule FixedCustomer do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string, width: 20)
      field(:email, :string, width: 30)
      embeds_one(:address, FixedAddress)
    end
  end

  defmodule WithDatetime do
    @moduledoc false
    use Delimit

    layout do
      field(:label, :string, width: 10)
      field(:timestamp, :datetime, width: 25)
    end
  end

  defmodule CustomTransformHelpers do
    @moduledoc false
    def read_tags(val), do: String.split(String.trim(val), "|")
    def write_tags(val), do: Enum.join(val, "|")
  end

  defmodule WithCustomFn do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string, width: 10)

      field(:tags, :string,
        width: 20,
        read_fn: &CustomTransformHelpers.read_tags/1,
        write_fn: &CustomTransformHelpers.write_tags/1
      )
    end
  end

  defmodule DualFormat do
    @moduledoc false
    use Delimit

    layout do
      field(:id, :integer, width: 10)
      field(:name, :string, width: 20)
      field(:score, :float, width: 10)
    end
  end

  defmodule SingleCharField do
    @moduledoc false
    use Delimit

    layout do
      field(:record_type, :string, width: 1)
      field(:flag, :boolean, width: 1)
      field(:code, :string, width: 3)
    end
  end

  defmodule NoWidthEmbed do
    @moduledoc false
    use Delimit

    # Embed references an address without width options
    defmodule NoWidthAddr do
      @moduledoc false
      use Delimit

      layout do
        field(:city, :string)
        field(:state, :string)
      end
    end

    layout do
      field(:name, :string, width: 10)
      embeds_one(:addr, NoWidthAddr)
    end
  end

  defmodule ZeroWidth do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string, width: 0)
    end
  end

  defmodule NegativeWidth do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string, width: -5)
    end
  end

  defmodule MissingWidth do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string, width: 10)
      field(:age, :integer)
    end
  end

  defmodule WithDefaults do
    @moduledoc false
    use Delimit

    layout do
      field(:name, :string, width: 10, default: "N/A")
      field(:count, :integer, width: 5, default: 0)
    end
  end
end
