///|
fn build_process_event(lease : @core.Lease) -> @core.ProcessEvent {
  {
    queue_name: lease.queue_name,
    task_name: lease.job.task_name,
    job_id: lease.job.job_id,
    dedupe_key: lease.job.dedupe_key,
    attempt: lease.job.attempt,
    max_attempts: lease.job.max_attempts,
    enqueued_at_ms: lease.job.created_at_ms,
    visible_at_ms: lease.job.visible_at_ms,
    started_at_ms: match lease.job.started_at_ms {
      Some(started_at_ms) => started_at_ms
      None => lease.leased_at_ms
    },
    deadline_ms: lease.job.deadline_ms,
  }
}

///|
fn build_process_result(
  outcome : @core.AttemptOutcome,
  error_message : String?,
  event : @core.ProcessEvent,
  finished_at_ms : Int64,
  next_visible_at_ms : Int64?,
) -> @core.ProcessResult {
  {
    outcome,
    error_message,
    duration_ms: finished_at_ms - event.started_at_ms,
    finished_at_ms,
    next_visible_at_ms,
  }
}

///|
fn[B] run_before_hooks(
  consumer : ConsumerService[B],
  event : @core.ProcessEvent,
) -> Unit {
  for hook in consumer.hooks {
    ignore(try? hook.before_process(event))
  }
}

///|
fn[B] run_after_hooks(
  consumer : ConsumerService[B],
  event : @core.ProcessEvent,
  result : @core.ProcessResult,
) -> Unit {
  for hook in consumer.hooks {
    ignore(try? hook.after_process(event, result))
  }
}
