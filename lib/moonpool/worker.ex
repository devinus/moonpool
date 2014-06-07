defmodule Moonpool.Worker do
  use Behaviour

  defcallback start_link(args :: Keyword.t) :: :gen.start_ret
end

