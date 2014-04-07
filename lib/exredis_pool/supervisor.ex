defmodule ExredisPool.Supervisor do
  use Supervisor.Behaviour

  def pool_spec(name, opts) do
    pool_args = [ name: { :local, name },
                  worker_module: :eredis ] ++ opts[:size_args]
    redis_args = opts[:redis_args]
    :poolboy.child_spec({ :local, name }, pool_args, redis_args)
  end

  def start_link do
    :supervisor.start_link(__MODULE__, [])
  end

  def init([]) do
    opts = :application.get_all_env
    children = [ pool_spec(ExredisPool.pool_name, opts) ]
    supervise(children, strategy: :one_for_one)
  end
end
