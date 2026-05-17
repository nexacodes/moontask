///|
priv suberror AttemptFailed {
  AttemptFailed(String)
} derive(Debug)

///|
priv enum FailureAction {
  Retried
  Dead
}

///|
fn deadline_ms_from(
  enqueue_time_ms : Int64,
  options : @taskq.EnqueueOptions,
) -> Int64? {
  match options.timeout_ms {
    Some(timeout_ms) => Some(enqueue_time_ms + Int64::from_int(timeout_ms))
    None => None
  }
}

///|
fn visible_at_ms_from(
  enqueue_time_ms : Int64,
  options : @taskq.EnqueueOptions,
) -> Int64 {
  match options.schedule_at_ms {
    Some(at_ms) => at_ms
    None => enqueue_time_ms + Int64::from_int(options.delay_ms)
  }
}

///|
fn timeout_remaining_ms(
  clock : @taskq.RuntimeClock,
  job : @taskq.JobEnvelope,
) -> Int? {
  match job.deadline_ms {
    Some(deadline_ms) =>
      Some(Int::max(0, (deadline_ms - (clock.now_ms)()).to_int()))
    None => None
  }
}

///|
fn retry_delay_ms(policy : @taskq.RetryPolicy, attempt : Int) -> Int? {
  match policy {
    @taskq.NoRetry => None
    @taskq.Fixed(delay_ms~, ..) => Some(delay_ms)
    @taskq.Exponential(initial_delay_ms~, factor~, max_delay_ms~, ..) => {
      let exponent = Double::from_int(attempt - 1)
      let scaled = initial_delay_ms.to_double() * @math.pow(factor, exponent)
      Some(Int::min(scaled.to_int(), max_delay_ms))
    }
  }
}

///|
fn next_retry_visible_at_ms(
  clock : @taskq.RuntimeClock,
  job : @taskq.JobEnvelope,
  policy : @taskq.RetryPolicy,
) -> Int64? {
  match retry_delay_ms(policy, job.attempt) {
    Some(delay_ms) => Some((clock.now_ms)() + Int64::from_int(delay_ms))
    None => None
  }
}

///|
async fn run_job_attempt(
  registry : @taskq.Registry,
  clock : @taskq.RuntimeClock,
  context : @taskq.TaskContext,
  job : @taskq.JobEnvelope,
) -> Result[Unit, Error] {
  match timeout_remaining_ms(clock, job) {
    Some(timeout_ms) =>
      try? @async.with_timeout(timeout_ms, () => {
        @taskq.run_registered_job(registry, context, job)
      })
    None => try? @taskq.run_registered_job(registry, context, job)
  }
}

///|
async fn run_job_fallback(
  registry : @taskq.Registry,
  context : @taskq.TaskContext,
  job : @taskq.JobEnvelope,
  error_message : String,
) -> Unit {
  @taskq.run_registered_fallback(
    registry,
    context,
    job,
    AttemptFailed(error_message),
  )
}
