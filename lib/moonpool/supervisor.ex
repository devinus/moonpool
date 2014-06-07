defmodule Moonpool.Supervisor do
  def start_link(module, args) do
    :supervisor.start_link(__MODULE__, { module, args })
  end

  def init({ module, args }) do
    { :ok, { { :simple_one_for_one, 0, 1 },
             [{ module, { module, :start_link, [args] },
                :temporary, 5000, :worker, [module] }] } }
  end
end

