defmodule OffBroadwayPgmq.MixProject do
  use Mix.Project

  @version "0.2.2"

  def project do
    [
      app: :off_broadway_pgmq,
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs(),
      description: description(),
      name: "OffBroadwayPgmq",
      source_url: "https://github.com/v0idpwn/off_broadway_pgmq"
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
      {:pgmq, "~> 0.2"},
      {:broadway, "~> 1.0"},
      {:ex_doc, ">= 0.0.0", runtime: false, only: :dev}
    ]
  end

  defp package do
    [
      name: "off_broadway_pgmq",
      files: ~w(lib .formatter.exs mix.exs README.md),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/v0idpwn/off_broadway_pgmq"}
    ]
  end

  defp docs do
    [
      main: "OffBroadwayPgmq",
      source_ref: "v#{@version}"
    ]
  end

  defp description, do: "Broadway producer for PGMQ"
end
