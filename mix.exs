defmodule Delimit.MixProject do
  use Mix.Project

  def project do
    [
      app: :delimit,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      name: "delimit",
      source_url: "https://github.com/advancedpricing/delimit"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # Development
      {:benchee, "~> 1.1", only: [:dev, :test]},
      {:benchee_html, "~> 1.0", only: [:dev, :test]},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:test, :dev], runtime: false},
      {:ex_doc, "~> 0.14", only: :dev, runtime: false},
      {:mix_test_watch, "~> 1.1", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.0", only: [:dev, :test], runtime: false},

      # Production
      {:nimble_csv, "~> 1.2"},
      {:timex, "~> 3.7"}
    ]
  end

  defp description do
    "Delimit is a powerful yet elegant library for reading and writing delimited data files (CSV, TSV, PSV, SSV) in Elixir. Inspired by Ecto, it allows you to define schemas for your delimited data, providing strong typing with structs, validation, and transformation capabilities. By defining the structure of your data, Delimit enables type-safe parsing and generation with minimal boilerplate code."
  end

  defp package do
    [
      licenses: ["LGPL-3.0-only"],
      links: %{"GitHub" => "https://github.com/advancedpricing/delimit"}
    ]
  end
end
