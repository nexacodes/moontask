///|
fn build_task_context(lease : @core.Lease) -> @core.TaskContext {
  {
    queue_name: lease.queue_name,
    job_id: lease.job.job_id,
    attempt: lease.job.attempt,
    max_attempts: lease.job.max_attempts,
    enqueued_at_ms: lease.job.created_at_ms,
    started_at_ms: match lease.job.started_at_ms {
      Some(started_at_ms) => started_at_ms
      None => lease.leased_at_ms
    },
    deadline_ms: lease.job.deadline_ms,
    dedupe_key: lease.job.dedupe_key,
  }
}

///|
fn running_lease_view(
  lease : @core.Lease,
  started_at_ms : Int64,
  deadline_ms : Int64?,
) -> @core.Lease {
  let running_job : @core.JobRecord = {
    ..lease.job,
    status: @core.Running,
    started_at_ms: Some(started_at_ms),
    deadline_ms,
  }
  { ..lease, job: running_job }
}

///|
fn attempt_deadline_ms(lease : @core.Lease, started_at_ms : Int64) -> Int64? {
  match lease.job.timeout_ms {
    Some(timeout_ms) => Some(started_at_ms + Int64::from_int(timeout_ms))
    None => None
  }
}

///|
fn is_attempt_timed_out(finished_at_ms : Int64, deadline_ms : Int64?) -> Bool {
  match deadline_ms {
    Some(deadline_ms) => finished_at_ms > deadline_ms
    None => false
  }
}

///|
async fn[B : @backend.QueueBackend] handle_attempt_failure(
  consumer : ConsumerService[B],
  lease : @core.Lease,
  context : @core.TaskContext,
  event : @core.ProcessEvent,
  err : Error,
) -> Unit {
  let retry_policy_result : Result[@core.RetryPolicy, Error] = try? @registry.registry_retry_policy(
    consumer.registry,
    lease.job.task_name,
  )
  let policy = match retry_policy_result {
    Ok(policy) => policy
    Err(_) => @core.NoRetry
  }
  match next_retry_visible_at_ms(consumer.clock, lease, policy) {
    Some(next_visible_at_ms) if lease.job.attempt < lease.job.max_attempts => {
      ignore(
        try? consumer.nack_retry_lease(
          lease,
          err.to_string(),
          next_visible_at_ms,
        ),
      )
      run_after_hooks(
        consumer,
        event,
        build_process_result(
          @core.FailedWillRetry,
          Some(err.to_string()),
          event,
          (consumer.clock.now_ms)(),
          Some(next_visible_at_ms),
        ),
      )
    }
    _ => {
      ignore(try? consumer.nack_dead_lease(lease, err.to_string()))
      ignore(
        try? @registry.run_registered_fallback(
          consumer.registry,
          context,
          lease.job,
          err,
        ),
      )
      run_after_hooks(
        consumer,
        event,
        build_process_result(
          @core.FailedDead,
          Some(err.to_string()),
          event,
          (consumer.clock.now_ms)(),
          None,
        ),
      )
    }
  }
}

///|
async fn[B : @backend.QueueBackend] process_lease(
  consumer : ConsumerService[B],
  lease : @core.Lease,
) -> Unit {
  let started_at_ms = (consumer.clock.now_ms)()
  let deadline_ms = attempt_deadline_ms(lease, started_at_ms)
  let running_lease_result : Result[@core.Lease, Error] = try? consumer.mark_running_lease(
    lease, started_at_ms, deadline_ms,
  )
  let running_lease = match running_lease_result {
    Ok(updated_lease) => updated_lease
    Err(_) => running_lease_view(lease, started_at_ms, deadline_ms)
  }
  let context = build_task_context(running_lease)
  let event = build_process_event(running_lease)
  run_before_hooks(consumer, event)
  let failure = try {
    @registry.run_registered_job(consumer.registry, context, running_lease.job)
    None
  } catch {
    err if @async.is_cancellation_error(err) => {
      // Forced shutdown should leave the lease recoverable instead of
      // translating cooperative task cancellation into a terminal failure.
      mark_lease_finished(consumer, lease.lease_id)
      return
    }
    err => Some(err)
  }
  let finished_at_ms = (consumer.clock.now_ms)()
  let failure = match failure {
    Some(err) => Some(err)
    None if is_attempt_timed_out(finished_at_ms, deadline_ms) =>
      Some(@core.QueueError::BackendFailure("task exceeded timeout"))
    None => None
  }
  match failure {
    None => {
      ignore(try? consumer.ack_lease(lease))
      run_after_hooks(
        consumer,
        event,
        build_process_result(@core.Succeeded, None, event, finished_at_ms, None),
      )
    }
    Some(err) => handle_attempt_failure(consumer, lease, context, event, err)
  }
}
