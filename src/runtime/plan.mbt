///|
pub(all) enum QueueLifecycle {
  Bootstrapping
  Running
  Closing
  Closed
} derive(Eq, ToJson, Debug)

///|
pub(all) struct RuntimePlan {
  lifecycle : QueueLifecycle
  max_concurrency : Int
  command_names : Array[String]
  notes : Array[String]
} derive(Eq, ToJson, Debug)

///|
pub fn is_accepting_commands(lifecycle : QueueLifecycle) -> Bool {
  match lifecycle {
    Running => true
    _ => false
  }
}

///|
pub fn can_dispatch_work(lifecycle : QueueLifecycle) -> Bool {
  match lifecycle {
    Running => true
    Closing => true
    _ => false
  }
}

///|
pub fn phase_two_runtime_plan(options : @taskq.QueueOptions) -> RuntimePlan {
  {
    lifecycle: Running,
    max_concurrency: options.max_concurrency,
    command_names: [
      "Enqueue", "Dispatch", "CompleteSuccess", "CompleteFailure", "PollStats", "Shutdown",
    ],
    notes: [
      "Phase two keeps a jobs table, ready queue, and inflight index for in-process delivery guarantees.",
      "Workers execute outside the queue state owner and report completion back through queue methods.",
    ],
  }
}
