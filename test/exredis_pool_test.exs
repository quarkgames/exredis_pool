defmodule ExredisPoolTest do
  use ExUnit.Case
  require ExredisPool

  test "set, get, and del" do
    assert({ :ok, "OK" } == ExredisPool.set("foo", "bar"))
    assert({ :ok, "bar" } == ExredisPool.get("foo"))
    assert({ :ok, "1" } == ExredisPool.del("foo"))
    assert({ :ok, :undefined } == ExredisPool.get("foo"))
  end

  test "multiset, multiget, and multi-del" do
    assert({ :ok, "OK" } == ExredisPool.mset(["a", 1, "b", 2, "c", 3]))
    assert({ :ok, ["1", "2", "3"] } == ExredisPool.mget(["a", "b", "c"]))
    assert({ :ok, "3" } == ExredisPool.del(["a", "b", "c"]))
    assert({ :ok, [:undefined, :undefined, :undefined] } ==
             ExredisPool.mget(["a", "b", "c"]))
  end

  test "transaction set, get, and delete" do
    ExredisPool.set("f", 3)
    res = ExredisPool.multi |> ExredisPool.set("d", 1)
                            |> ExredisPool.set("e", 2)
                            |> ExredisPool.get("f")
                            |> ExredisPool.exec
    assert({ :ok, [ "OK", "OK", "3" ] } == res)
    res = ExredisPool.multi |> ExredisPool.del("e")
                            |> ExredisPool.del("d")
                            |> ExredisPool.del("f")
                            |> ExredisPool.exec
    assert({ :ok, [ "1", "1", "1" ] } == res)
  end

  test "pipeline set, get, and delete" do
    ExredisPool.set("f", 3)
    res = ExredisPool.pipe |> ExredisPool.set("d", 1)
                           |> ExredisPool.set("e", 2)
                           |> ExredisPool.get("f")
                           |> ExredisPool.line
    assert([ {:ok, "OK"}, {:ok, "OK"}, {:ok, "3"} ] == res)
    res = ExredisPool.pipe |> ExredisPool.del("e")
                           |> ExredisPool.del("d")
                           |> ExredisPool.del("f")
                           |> ExredisPool.line
    assert([ { :ok, "1"}, { :ok, "1"}, { :ok, "1" } ] == res)
  end
end
