defmodule ExredisPool.Mixfile do
  use Mix.Project

  def project do
    [ app: :exredis_pool,
      version: "0.0.1",
      elixir: "~> 0.12.5",
      deps: deps ]
  end

  # Configuration for the OTP application
  def application do
    [mod: { ExredisPool, [] },
     env: [
           size_args: [ size: 10,
                        max_overflow: 30 ],
           redis_args: [ "127.0.0.1", 6379 ]
       ]
    ]
  end

  # Returns the list of dependencies in the format:
  # { :foobar, git: "https://github.com/elixir-lang/foobar.git", tag: "0.1" }
  #
  # To specify particular versions, regardless of the tag, do:
  # { :barbat, "~> 0.1", github: "elixir-lang/barbat" }
  defp deps do
    [
     { :eredis, github: "wooga/eredis" },
     { :poolboy, github: "devinus/poolboy" }
    ]
  end
end
