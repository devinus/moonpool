defmodule Moonpool do
  use GenServer.Behaviour
  alias Moonpool.Supervisor

  @default_size 0
  @default_max_overflow 0

  defrecord State,
    supervisor: nil,
    workers: nil,
    waiting: nil,
    monitors: nil,
    size: @default_size,
    overflow: 0,
    max_overflow: @default_max_overflow

  def checkout(pool) do
    :gen_server.call pool, :checkout
  end

  def checkin(pool, worker) do
    :gen_server.cast pool, { :checkin, worker }
  end

  def start_link(pool_args, worker_args) do
    name = Keyword.fetch!(pool_args, :name)
    :gen_server.start_link name, __MODULE__, { pool_args, worker_args }, []
  end

  @doc false
  def init({ pool_args, worker_args }) do
    Process.flag(:trap_exit, true)

    worker_module = Keyword.fetch!(pool_args, :worker_module)
    { :ok, supervisor } = Supervisor.start_link(worker_module, worker_args)

    size = pool_args[:size] || @default_size
    max_overflow = pool_args[:max_overflow] || @default_max_overflow

    workers = prepopulate(supervisor, size)

    waiting = :queue.new
    monitors = :ets.new(:monitors, [ :private ])

    state = State[supervisor: supervisor,
                  workers: workers,
                  waiting: waiting,
                  monitors: monitors,
                  size: size,
                  max_overflow: max_overflow]

    { :ok, state }
  end

  def handle_call(:checkout, { from_pid, _ } = from, State[] = state) do
    State[overflow: overflow,
          max_overflow: max_overflow] = state
    case :queue.out(state.workers) do
      { { :value, worker }, left } ->
        ref = Process.monitor(from_pid)
        true = :ets.insert(state.monitors, { worker, ref })
        { :reply, worker, state.update(workers: left) }
      { :empty, empty } when max_overflow > 0 and overflow < max_overflow ->
        { worker, ref } = new_worker(state.supervisor, from_pid)
        true = :ets.insert(state.monitors, { worker, ref })
        { :reply, worker, state.update(workers: empty, overflow: overflow + 1) }
      { :empty, empty } ->
        waiting = :queue.in(from_pid, state.waiting)
        { :noreply, state.update(workers: empty, waiting: waiting) }
    end
  end

  def handle_cast({ :checkin, worker }, State[] = state) do
    monitors = state.monitors
    new_state = case :ets.lookup(monitors, worker) do
      [{ worker, ref }] ->
        true = Process.demonitor(ref)
        true = :ets.delete(monitors, worker)

        State[overflow: overflow] = state
        case :queue.out(state.waiting) do
          { { :value, { from_pid, _ } = from }, left } ->
            ref = Process.monitor(from_pid)
            true = :ets.insert(state.monitors, { worker, ref })
            :gen_server.reply(from, worker)
            state.update(waiting: left)
          { :empty, empty } when overflow > 0 ->
            :ok = dismiss_worker(state.supervisor, worker)
            state.update(waiting: empty, overflow: overflow - 1)
          { :empty, empty } ->
            workers = :queue.in(worker, state.workers)
            state.update(workers: workers, waiting: empty, overflow: 0)
        end
      [] -> state
    end
    { :noreply, new_state }
  end

  def handle_info({ :DOWN, ref, _, _, _ }, State[] = state) do
    new_state = case :ets.match(state.monitors, { :"$1", ref }) do
      [[ worker ]] ->
        :ok = :supervisor.terminate_child(state.supervisor, worker)
        true = :ets.delete(state.monitors, worker)
        handle_worker_exit(worker, state)
      [] ->
        state
    end
    { :noreply, state }
  end

  def handle_info({ :EXIT, worker, _ }, State[] = state) do
    monitors = state.monitors
    new_state = case :ets.lookup(monitors, worker) do
      [{ worker, ref }] ->
        true = Process.demonitor(ref)
        true = :ets.delete(monitors, worker)
        handle_worker_exit(worker, state)
      [] ->
        workers = state.workers
        case :queue.member(worker, workers) do
          true ->
            workers = :queue.filter(&(&1 != worker), workers)
            workers = :queue.in(new_worker(state.supervisor), workers)
            state.update(workers: workers)
          false ->
            state
        end
    end
    { :noreply, new_state }
  end

  defp handle_worker_exit(worker, State[] = state) do
    overflow = state.overflow
    case :queue.out(state.waiting) do
      { { :value, { from_pid, _ } = from }, left } ->
        ref = Process.monitor(from_pid)
        new_worker = new_worker(state.supervisor)
        true = :ets.insert(state.monitors, { new_worker, ref })
        :gen_server.reply(from, new_worker)
        state.update(waiting: left)
      { :empty, empty } when overflow > 0 ->
        state.update(overflow: overflow - 1, waiting: empty)
      { :empty, empty } ->
        workers = :queue.filter(&(&1 != worker), state.workers)
        workers = :queue.in(new_worker(state.supervisor), workers)
        state.update(workers: workers, waiting: empty)
    end
  end

  defp prepopulate(supervisor, size) do
    Enum.reduce 1..size, :queue.new, fn _, workers ->
      :queue.in(new_worker(supervisor), workers)
    end
  end

  defp new_worker(supervisor) do
    { :ok, pid } = :supervisor.start_child(supervisor, [])
    Process.link(pid)
    pid
  end

  defp new_worker(supervisor, from_pid) do
    pid = new_worker(supervisor)
    ref = Process.monitor(from_pid)
    { pid, ref }
  end

  defp dismiss_worker(supervisor, pid) do
    Process.unlink(pid)
    :supervisor.terminate_child(supervisor, pid)
  end
end
