///|
enum AttemptResult {
  Succeeded(
    lease~ : @core.Lease,
    event~ : @core.ProcessEvent,
    finished_at_ms~ : Int64
  )
  Failed(
    lease~ : @core.Lease,
    context~ : @core.TaskContext,
    event~ : @core.ProcessEvent,
    finished_at_ms~ : Int64,
    err~ : Error
  )
}

///|
fn[B] ConsumerService::reset_runtime_metrics(self : ConsumerService[B]) -> Unit {
  self.reserve_calls = 0
  self.reserved_jobs = 0
  self.reserve_latency_ms_total = 0L
  self.ack_calls = 0
  self.ack_latency_ms_total = 0L
  self.nack_retry_calls = 0
  self.nack_dead_calls = 0
  self.lease_extensions = 0
  self.expired_leases_reaped = 0
  self.processed_attempts = 0
  self.succeeded_attempts = 0
  self.failed_retry_attempts = 0
  self.failed_dead_attempts = 0
  self.cancelled_attempts = 0
}

///|
fn[B] ConsumerService::close_result_queue(self : ConsumerService[B]) -> Unit {
  match self.result_queue {
    Some(queue) => queue.close(clear=false)
    None => ()
  }
}

///|
fn[B] ConsumerService::submit_attempt_result(
  self : ConsumerService[B],
  result : AttemptResult,
) -> Unit raise {
  let queue = match self.result_queue {
    Some(queue) => queue
    None =>
      raise @core.QueueError::StateInvariant(
        "consumer result queue is not initialized",
      )
  }
  self.pending_ack_count = self.pending_ack_count + 1
  guard queue.try_put(result) else {
    self.pending_ack_count = Int::max(0, self.pending_ack_count - 1)
    raise @core.QueueError::StateInvariant(
      "consumer result queue rejected item",
    )
  }
}

///|
async fn[B : @backend.QueueBackend] ConsumerService::ack_lease(
  self : ConsumerService[B],
  lease : @core.Lease,
  finished_at_ms : Int64,
) -> Unit {
  let started_at_ms = (self.clock.now_ms)()
  self.backend.ack(lease.lease_id, finished_at_ms)
  self.ack_calls = self.ack_calls + 1
  self.ack_latency_ms_total = self.ack_latency_ms_total +
    ((self.clock.now_ms)() - started_at_ms)
  mark_lease_finished(self, lease.lease_id)
}

///|
pub async fn[B : @backend.QueueBackend] ConsumerService::nack_retry_lease(
  self : ConsumerService[B],
  lease : @core.Lease,
  error_message : String,
  next_visible_at_ms : Int64,
  finished_at_ms : Int64,
) -> Unit {
  self.backend.nack_retry(
    lease.lease_id,
    error_message,
    next_visible_at_ms,
    finished_at_ms,
  )
  self.nack_retry_calls = self.nack_retry_calls + 1
  mark_lease_finished(self, lease.lease_id)
}

///|
pub async fn[B : @backend.QueueBackend] ConsumerService::nack_dead_lease(
  self : ConsumerService[B],
  lease : @core.Lease,
  error_message : String,
  finished_at_ms : Int64,
) -> Unit {
  self.backend.nack_dead(lease.lease_id, error_message, finished_at_ms)
  self.nack_dead_calls = self.nack_dead_calls + 1
  mark_lease_finished(self, lease.lease_id)
}

///|
async fn[B : @backend.QueueBackend] apply_success_result(
  consumer : ConsumerService[B],
  lease : @core.Lease,
  event : @core.ProcessEvent,
  finished_at_ms : Int64,
) -> Unit {
  ignore(try? consumer.ack_lease(lease, finished_at_ms))
  consumer.processed_attempts = consumer.processed_attempts + 1
  consumer.succeeded_attempts = consumer.succeeded_attempts + 1
  run_after_hooks(
    consumer,
    event,
    build_process_result(@core.Succeeded, None, event, finished_at_ms, None),
  )
}

///|
async fn[B : @backend.QueueBackend] apply_failure_result(
  consumer : ConsumerService[B],
  lease : @core.Lease,
  context : @core.TaskContext,
  event : @core.ProcessEvent,
  finished_at_ms : Int64,
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
          finished_at_ms,
        ),
      )
      consumer.processed_attempts = consumer.processed_attempts + 1
      consumer.failed_retry_attempts = consumer.failed_retry_attempts + 1
      run_after_hooks(
        consumer,
        event,
        build_process_result(
          @core.FailedWillRetry,
          Some(err.to_string()),
          event,
          finished_at_ms,
          Some(next_visible_at_ms),
        ),
      )
    }
    _ => {
      ignore(
        try? consumer.nack_dead_lease(lease, err.to_string(), finished_at_ms),
      )
      ignore(
        try? @registry.run_registered_fallback(
          consumer.registry,
          context,
          lease.job,
          err,
        ),
      )
      consumer.processed_attempts = consumer.processed_attempts + 1
      consumer.failed_dead_attempts = consumer.failed_dead_attempts + 1
      run_after_hooks(
        consumer,
        event,
        build_process_result(
          @core.FailedDead,
          Some(err.to_string()),
          event,
          finished_at_ms,
          None,
        ),
      )
    }
  }
}

///|
async fn[B : @backend.QueueBackend] ConsumerService::run_ack_loop(
  self : ConsumerService[B],
) -> Unit {
  let queue = match self.result_queue {
    Some(queue) => queue
    None => return
  }
  while true {
    let result : AttemptResult = queue.get() catch { _ => return }
    self.pending_ack_count = Int::max(0, self.pending_ack_count - 1)
    match result {
      Succeeded(lease~, event~, finished_at_ms~) =>
        apply_success_result(self, lease, event, finished_at_ms)
      Failed(lease~, context~, event~, finished_at_ms~, err~) =>
        apply_failure_result(self, lease, context, event, finished_at_ms, err)
    }
  }
}
