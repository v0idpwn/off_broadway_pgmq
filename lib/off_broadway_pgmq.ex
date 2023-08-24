defmodule OffBroadwayPgmq do
  @moduledoc """
  Pgmq producer for broadway, adapted from BroadwaySQS

  The producer receives 4 options:
  - `:repo`: the ecto repo to be used, mandatory.
  - `:queue`: the queue name to be used, mandatory.
  - `:max_poll_seconds`: how long the maximum poll request takes, optional, defaults
  to 5 seconds.
  - `:attempt_interval_ms`: interval in ms to wait before doing poll requests in
  case there is demand but no messages are found. Optional, defaults to 500
  - `:pgmq_poll_interval_ms`: option on pgmq side that dictates poll interval
  postgres-side. Optional, defaults to 250.

  If you're using many queues, this can be a bit heavy in your connection pool,
  so its important to configure properly. You might want to adjust `max_poll_seconds`
  and `:attempt_interval_ms` to trade off connection usage for more latency. You can
  also use a `:max_poll_seconds` of 0 to perform no polling at all.

  `:pgmq_poll_interval_ms` is the database side poll interval. By adjusting it,
  you can increase or decrease the amount of work performed database side at
  the risk of getting more latency.
  """

  use GenStage

  @default_max_poll_seconds 5
  @default_pgmq_poll_interval_ms 250
  @default_attempt_interval_ms 500

  alias Broadway.Producer

  @behaviour Producer

  @impl true
  def init(opts) do
    repo = Keyword.fetch!(opts, :repo)
    queue = Keyword.fetch!(opts, :queue)
    max_poll_seconds = Keyword.get(opts, :max_poll_seconds, @default_max_poll_seconds)
    poll_interval_ms = Keyword.get(opts, :db_poll_interval_ms, @default_pgmq_poll_interval_ms)
    attempt_interval_ms = Keyword.get(opts, :attempt_interval_ms, @default_attempt_interval_ms)

    {:producer,
     %{
       demand: 0,
       receive_timer: nil,
       receive_interval: attempt_interval_ms,
       repo: repo,
       queue: queue,
       max_poll_seconds: max_poll_seconds,
       poll_interval_ms: poll_interval_ms
     }}
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl true
  def handle_info(:receive_messages, %{receive_timer: nil} = state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_info(:receive_messages, state) do
    handle_receive_messages(%{state | receive_timer: nil})
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @impl Producer
  def prepare_for_draining(%{receive_timer: receive_timer} = state) do
    receive_timer && Process.cancel_timer(receive_timer)
    {:noreply, [], %{state | receive_timer: nil}}
  end

  defp handle_receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    messages = receive_messages(state, demand)
    new_demand = demand - length(messages)

    receive_timer =
      case {messages, new_demand} do
        {[], _} -> schedule_receive_messages(state.attempt_interval_ms)
        {_, 0} -> nil
        _ -> schedule_receive_messages(0)
      end

    {:noreply, messages, %{state | demand: new_demand, receive_timer: receive_timer}}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp receive_messages(s, total_demand) do
    :telemetry.span(
      [:off_broadway_pgmq, :receive_messages],
      %{},
      fn ->
        messages =
          Pgmq.read_messages_with_poll(
            s.repo,
            s.queue,
            s.visibility_timeout,
            total_demand,
            s.max_poll_seconds,
            s.poll_interval_ms
          )

        {messages, %{messages: messages}}
      end
    )
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end
end
