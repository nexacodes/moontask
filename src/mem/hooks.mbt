///|
fn clock_now_ms(queue : MemQueue) -> Int64 {
  (queue.clock.now_ms)()
}

///|
async fn sleep_with_clock(queue : MemQueue, delay_ms : Int) -> Unit {
  (queue.clock.sleep_ms)(delay_ms)
}

///|
fn build_process_event(lease : WorkerLease) -> @taskq.ProcessEvent {
  {
    queue_name: lease.context.queue_name,
    task_name: lease.job.task_name,
    job_id: lease.job.id,
    dedupe_key: lease.job.dedupe_key,
    attempt: lease.job.attempt,
    max_attempts: lease.job.max_attempts,
    enqueued_at_ms: lease.job.created_at_ms,
    visible_at_ms: lease.job.visible_at_ms,
    started_at_ms: lease.context.started_at_ms,
    deadline_ms: lease.job.deadline_ms,
  }
}

///|
fn build_process_result(
  outcome : @taskq.AttemptOutcome,
  error_message : String?,
  started_at_ms : Int64,
  finished_at_ms : Int64,
  next_visible_at_ms : Int64?,
) -> @taskq.ProcessResult {
  {
    outcome,
    error_message,
    duration_ms: finished_at_ms - started_at_ms,
    finished_at_ms,
    next_visible_at_ms,
  }
}

///|
fn record_hook_result(queue : MemQueue, result : Result[Unit, Error]) -> Unit {
  match result {
    Ok(_) => ()
    Err(_) => queue.hook_errors = queue.hook_errors + 1
  }
}

///|
fn run_before_hooks(queue : MemQueue, event : @taskq.ProcessEvent) -> Unit {
  for hook in queue.hooks {
    let result : Result[Unit, Error] = try? hook.before_process(event)
    record_hook_result(queue, result)
  }
}

///|
fn run_after_hooks(
  queue : MemQueue,
  event : @taskq.ProcessEvent,
  result : @taskq.ProcessResult,
) -> Unit {
  for hook in queue.hooks {
    let hook_result : Result[Unit, Error] = try? hook.after_process(
      event, result,
    )
    record_hook_result(queue, hook_result)
  }
}
