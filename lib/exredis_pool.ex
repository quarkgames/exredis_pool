defmodule ExredisPool do
  use Application.Behaviour

  def pool_name, do: :__exredis_pool__

  commands = [
              # keys

              del: "1n", del: 1, dump: 1, exists?: 1, expire: 2,
              expireat: 2, keys: 1, move: 2, persist: 1, pexpire: 2,
              pexpireat: 2, pttl: 1, randomkey: 0, rename: 2,
              renamenx: 2, restore: 3, ttl: 1, type: 1,

              # strings

              append: 2, bitcount: 1, bitcount: 3, bitop: 3,
              bitpos: 2, bitpos: 4, decr: 1, decrby: 2,
              get: 1, getbit: 2, getrange: 3, getset: 2,
              incr: 1, incrby: 2, incrbyfloat: 2, mget: "1n",
              mset: "2n", msetnx: "2n", psetex: 3, set: 2,
              set: 4, set: 6,
              setbit: 3, setex: 3, setnx: 2, setrange: 3,
              strlen: 1,

              # hashes

              hdel: 2, hexists?: 2, hget: 2, hgetall: 1,
              hincrby: 3, hincrbyfloat: 3, hkeys: 1, hlen: 1,
              hmget: [1, "1n"], hmset: [1, "2n"], hset: 3, hsetnx: 3,
              hvals: 1, hscan: 4, hscan: 6,

              # lists

              blpop: ["1n", 1], blpop: 2, brpop: ["1n", 1], brpop: 2,
              brpoplpush: 3, lindex: 2, linsert: 4, llen: 1,
              lpop: 1, lpush: [1, "1n"], lpush: 2, lpushx: 2,
              lrange: 3, lrem: 3, lset: 3, ltrim: 3,
              rpop: 1, rpoplpush: 1, rpush: [1, "1n"], rpush: 2,
              rpushx: 2,

              # sets

              sadd: [1, "1n"], sadd: 2, scard: 1, sdiff: [1, "1n"],
              sdiff: 2, sdiffstore: [2, "1n"], sdiffstore: 3, sinter: [1, "1n"],
              sinter: 2, sinterstore: [2, "1n"], sinterstore: 3, sismember?: 2,
              smembers: 1, smove: 3, spop: 1, srandmember: [1, "1n"],
              srandmember: 2, srem: [1, "1n"], srem: 2,
              sunion: "1n", sunion: 2, sunionstore: [1, "1n"],
              sunionstore: 3, sscan: 4, sscan: 6,

              # sorted sets (zinterstore and zunionstore are defined directly)
              zadd: [1, "2n"], zadd: 3, zcard: 1, zcount: 3, zincrby: 3,
              zrange: 3, zrange: 4, zrangebyscore: 3, zrangebyscore: 4,
              zrangebyscore: 7, zrank: 2,
              zrem: [1, "1n"], zrem: 2, zremrangebyrank: 3, zremrangebyscore: 3,
              zrevrange: 3, zrevrange: 4, zrevrangebyscore: 3,
              zrevrangebyscore: 4, zrevrangebyscore: 7,
              zrevrank: 2, zscore: 2, zscan: 4, zscan: 6,

              # hyperloglog

              pfadd: 2, pfcount: 1, pfmerge: [1, "1n"], pfmerge: 2,

              # transactions
              watch: "1n", watch: 1, unwatch: 0,

              # connection

              auth: 1, ping: 0, select: 1, echo: 1,

              # server

              bgrewriteaof: 0, bgsave: 0, client_kill: 1, client_list: 0,
              client_getname: 0, client_pause: 1, client_setname: 1,
              config_get: 1, config_rewrite: 0, config_set: 2,
              config_resetstate: 0, dbsize: 0, debug_object: 1,
              debug_segfault: 1, flushall: 0, flushdb: 0, info: 1 , lastsave: 0,
              save: 0, slaveof: 2, time: 0
             ]


  defmodule Generator do
    @module ExredisPool
    def pool_name, do: :__exredis_pool__

    def generate_command({ raw_name, arg_spec }, n) do
      command = to_redis_command(raw_name)
      quoted = generate_command(raw_name, command, arg_spec)
      Module.eval_quoted @module, quoted, [], file: __ENV__.file, line: n
      n+1
    end
    defp generate_command(name, command, 0) do
      quote do
        def unquote(name)() do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn, unquote(var!(command)))
                               end)
        end
        def unquote(name)({:pipe, pipe}) do
          { :pipe, [ unquote(var!(command)) | pipe ] }
        end
      end
    end
    defp generate_command(name, command, 1) do
      quote do
        def unquote(name)(a) do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(var!(command)
                                                     ++ [quote do: a]))
                               end)
        end
        def unquote(name)({:pipe, pipe}, a) do
          { :pipe, [ unquote(var!(command) ++ [quote do: a]) | pipe ] }
        end
      end
    end
    defp generate_command(name, command, 2) do
      quote do
        def unquote(name)(a, b) do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(var!(command) ++
                                                     quote do: [a, b]))
                               end)
        end
        def unquote(name)({:pipe, pipe}, a, b) do
          { :pipe, [ unquote(var!(command) ++ quote do: [a, b]) | pipe ] }
        end
      end
    end
    defp generate_command(name, command, 3) do
      quote do
        def unquote(name)(a, b, c) do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(var!(command) ++
                                                     quote do: [a, b, c]))
                               end)
        end
        def unquote(name)({:pipe, pipe}, a, b, c) do
          { :pipe, [ unquote(var!(command) ++ quote do: [a, b, c]) | pipe ] }
        end
      end
    end
    defp generate_command(name, command, 4) do
      quote do
        def unquote(name)(a, b, c, d) do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(var!(command) ++
                                                     quote do: [a, b, c, d]))
                               end)
        end
        def unquote(name)({:pipe, pipe}, a, b, c, d) do
          { :pipe, [ unquote(var!(command) ++ quote do: [a, b, c, d]) | pipe ] }
        end
      end
    end
    defp generate_command(name, command, 5) do
      quote do
        def unquote(name)(a, b, c, d, e) do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(var!(command) ++
                                                     quote do: [a, b, c, d, e]))
                               end)
        end
        def unquote(name)({:pipe, pipe}, a, b, c, d, e) do
          { :pipe, [ unquote(var!(command) ++ quote do: [a, b, c, d, e]) | pipe ] }
        end
      end
    end
    defp generate_command(name, command, 6) do
      quote do
        def unquote(name)(a, b, c, d, e, f) do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(var!(command) ++
                                                     quote do: [a, b, c, d, e, f]))
                               end)
        end
        def unquote(name)({:pipe, pipe}, a, b, c, d, e, f) do
          { :pipe, [ unquote(var!(command) ++ quote do: [a, b, c, d, e, f]) | pipe ] }
        end
      end
    end
    defp generate_command(name, command, 7) do
      quote do
        def unquote(name)(a, b, c, d, e, f, g) do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(var!(command) ++
                                                     quote do: [a, b, c, d , e, f, g]))
                               end)
        end
        def unquote(name)({:pipe, pipe}, a, b, c, d, e, f, g) do
          { :pipe, [ unquote(var!(command) ++ quote do: [a, b, c, d, e, f, g]) | pipe ] }
        end
      end
    end
    defp generate_command(name, command, "1n") do
      quote do
        def unquote(name)(a) when is_list(a) do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(var!(command)) ++ a)
                               end)
        end
        def unquote(name)({:pipe, pipe}, a) when is_list(a) do
          { :pipe, [ unquote(var!(command)) ++ a | pipe ] }
        end
      end
    end
    defp generate_command(name, command, "2n") do
      quote do
        defmacro unquote(name)(a)
        when is_list(a)
        when rem(length(a), 2) == 0 do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(var!(command)) ++ a)
                               end)
        end
        def unquote(name)({:pipe, pipe}, a)
        when is_list(a)
        when rem(length(a), 2) == 0 do
          { :pipe, [ unquote(var!(command)) ++ a | pipe ] }
        end
      end
    end
    defp generate_command(name, command, [1, "1n"]) do
      quote do
        def unquote(name)(a, bs) when is_list(bs) do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.
                                   q(conn, unquote(var!(command) ++ [quote do: a])
                                     ++ bs)
                               end)
        end
        def unquote(name)({:pipe, pipe}, a, bs) when is_list(bs) do
          { :pipe, [ unquote(var!(command) ++ [quote do: a]) ++ bs | pipe ] }
        end
      end
    end
    defp generate_command(name, command, [2, "1n"]) do
      quote do
        def unquote(name)(a, b, cs) when is_list(cs) do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(command ++
                                                       quote do: [a, b]) ++ cs)
                               end)
        end
        def unquote(name)({:pipe, pipe}, a, b, cs) when is_list(cs) do
          { :pipe, [ unquote(var!(command) ++ quote do: [a, b]) ++ cs | pipe ] }
        end
      end
    end
    defp generate_command(name, command, [1, "2n"]) do
      quote do
        def unquote(name)(a, bs) when rem(length(bs), 2) == 0 do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(var!(command) ++ [quote do: a])
                                             ++ bs)
                               end)
        end
        def unquote(name)({:pipe, pipe}, a, bs)
        when is_list(bs)
        when rem(length(bs), 2) == 0 do
          { :pipe, [ unquote(var!(command) ++ [quote do: a]) ++ bs | pipe ] }
        end
      end
    end
    defp generate_command(name, command, [2, "2n"]) do
      quote do
        def unquote(name)(a, b, cs)
        when is_list(cs)
        when rem(length(cs), 2) == 0 do
          :poolboy.transaction(unquote(pool_name),
                               fn(conn) ->
                                   :eredis.q(conn,
                                             unquote(var!(command) ++
                                                     quote do: [a, b]) ++ cs)
                               end)
        end
        def unquote(name)({:pipe, pipe}, a, b, cs)
        when is_list(cs)
        when rem(length(cs), 2) == 0 do
          { :pipe, [ unquote(var!(command) ++ quote do: [a, b]) ++ cs | pipe ] }
        end
      end
    end
    defp generate_command(_name, _command, _arg_spec) do
    end

    defp to_redis_command(raw_name) do
      raw_name |> atom_to_binary
               |> String.upcase
               |> String.rstrip(??)
               |> String.split("_")
    end
  end

  def zinterstore(dest, keys) do
    zinterstore(dest, keys, :sum)
  end
  def zinterstore(dest, keys, :sum) do
    query = ["ZINTERSTORE", dest, length(keys) | keys]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end
  def zinterstore(dest, keys, :min) do
    query = ["ZINTERSTORE", dest, length(keys) | keys] ++ ["AGGREGATE", "MIN"]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end
  def zinterstore(dest, keys, :max) do
    query = ["ZINTERSTORE", dest, length(keys) | keys] ++ ["AGGREGATE", "MAX"]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end
  def zinterstore(dest, keys, weights) do
    zinterstore(dest, keys, weights, :sum)
  end
  def zinterstore(dest, keys, weights, :sum)
  when length(keys) == length(weights) do
    query = ["ZINTERSTORE", dest, length(keys) | keys] ++ ["WEIGHTS" | weights]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end
  def zinterstore(dest, keys, weights, :min)
  when length(keys) == length(weights) do
    query = ["ZINTERSTORE", dest, length(keys) | keys]
    ++ ["WEIGHTS" | weights] ++ ["AGGREGATE", "MIN"]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end
  def zinterstore(dest, keys, weights, :max)
  when length(keys) == length(weights) do
    query = ["ZINTERSTORE", dest, length(keys) | keys]
    ++ ["WEIGHTS" | weights] ++ ["AGGREGATE", "MAX"]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end

  def zunionstore(dest, keys) do
    zunionstore(dest, keys, :sum)
  end
  def zunionstore(dest, keys, :sum) do
    query = ["ZUNIONSTORE", dest, length(keys) | keys]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end
  def zunionstore(dest, keys, :min) do
    query = ["ZUNIONSTORE", dest, length(keys) | keys] ++ ["AGGREGATE", "MIN"]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end
  def zunionstore(dest, keys, :max) do
    query = ["ZUNIONSTORE", dest, length(keys) | keys] ++ ["AGGREGATE", "MAX"]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end
  def zunionstore(dest, keys, weights) do
    zunionstore(dest, keys, weights, :sum)
  end
  def zunionstore(dest, keys, weights, :sum)
  when length(keys) == length(weights) do
    query = ["ZUNIONSTORE", dest, length(keys) | keys] ++ ["WEIGHTS" | weights]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end
  def zunionstore(dest, keys, weights, :min)
  when length(keys) == length(weights) do
    query = ["ZUNIONSTORE", dest, length(keys) | keys]
    ++ ["WEIGHTS" | weights] ++ ["AGGREGATE", "MIN"]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end
  def zunionstore(dest, keys, weights, :max)
  when length(keys) == length(weights) do
    query = ["ZUNIONSTORE", dest, length(keys) | keys]
    ++ ["WEIGHTS" | weights] ++ ["AGGREGATE", "MAX"]
    :poolboy.transaction(pool_name,
                         fn(conn) ->
                             :eredis.q(conn, query)
                         end)
  end

  List.foldl commands, 0, &Generator.generate_command/2

  # See http://elixir-lang.org/docs/stable/Application.Behaviour.html
  # for more information on OTP Applications
  def start(_type, _args) do
    ExredisPool.Supervisor.start_link
  end

  def pipe do
    { :pipe, [] }
  end

  defp unwrap([ { :ok, "OK" }, { :ok, "QUEUED" } | rest ]) do
    unwrap1(rest)
  end
  defp unwrap(result), do: result

  defp unwrap1([ { :ok, "QUEUED" } | rest ]) do
    unwrap1(rest)
  end
  defp unwrap1([rest]), do: rest

  def line({ :pipe, query }) do
    query = Enum.reverse(query)
    res = :poolboy.transaction(pool_name,
                               fn(conn) ->
                                   :eredis.qp(conn, query)
                               end)
    unwrap(res)
  end

  def multi do
    { :pipe, [ [ "MULTI" ] ] }
  end
  def multi({ :pipe, pipe }) do
    { :pipe, [ [ "MULTI" ] | pipe ] }
  end

  def exec({ :pipe, pipe }) do
    line({ :pipe, [ [ "EXEC" ] | pipe ]})
  end

end
