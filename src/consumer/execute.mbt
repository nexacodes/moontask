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
  let attempt_failure = try {
    @registry.run_registered_job(consumer.registry, context, running_lease.job)
    None
  } catch {
    err if @async.is_cancellation_error(err) => {
      mark_lease_finished(consumer, lease.lease_id)
      consumer.cancelled_attempts = consumer.cancelled_attempts + 1
      return
    }
    err => Some(err)
  }
  let finished_at_ms = (consumer.clock.now_ms)()
  let attempt_failure = match attempt_failure {
    Some(err) => Some(err)
    None if is_attempt_timed_out(finished_at_ms, deadline_ms) =>
      Some(@core.QueueError::BackendFailure("task exceeded timeout"))
    None => None
  }
  let result = match attempt_failure {
    None => Some(Succeeded(lease=running_lease, event~, finished_at_ms~))
    Some(err) =>
      Some(Failed(lease=running_lease, context~, event~, finished_at_ms~, err~))
  }
  match result {
    None => ()
    Some(result) => {
      let submission_result : Result[Unit, Error] = try? consumer.submit_attempt_result(
        result,
      )
      if submission_result is Err(_) {
        mark_lease_finished(consumer, lease.lease_id)
      }
    }
  }
}
