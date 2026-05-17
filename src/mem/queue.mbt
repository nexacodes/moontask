///|

///|
pub(all) struct WorkerLease {
  job : @taskq.JobEnvelope
  context : @taskq.TaskContext
} derive(Eq, ToJson, Debug)

///|
priv struct WorkerOutcome {
  lease : WorkerLease
  result : Result[Unit, Error]
}

///|
priv enum CommandReply {
  Done
  Lease(WorkerLease?)
  Snapshot(@taskq.StatsSnapshot)
  Failure(FailureAction)
}

///|
pub struct MemQueue {
  name : String
  registry : @taskq.Registry
  options : @taskq.QueueOptions
  hooks : Array[&@taskq.Hook]
  clock : @taskq.RuntimeClock
  plan : @runtime.RuntimePlan
  mut lifecycle : @runtime.QueueLifecycle
  jobs : Map[String, @taskq.JobEnvelope]
  dedupe_index : Map[String, @taskq.JobId]
  ready_queue : Array[@taskq.JobId]
  delayed_queue : Array[@taskq.JobId]
  inflight : Map[String, @taskq.JobEnvelope]
  mut next_job_id : Int
  mut processed : Int
  mut succeeded : Int
  mut retried : Int
  mut failed : Int
  mut dead : Int
  mut paused : Bool
  mut consecutive_failures : Int
  mut rate_tokens : Double
  mut rate_last_refill_ms : Int64
  mut hook_errors : Int
}

///|
pub fn MemQueue::new(
  registry : @taskq.Registry,
  options : @taskq.QueueOptions,
) -> MemQueue {
  MemQueue::new_with_runtime(
    registry,
    options,
    @taskq.default_runtime_options(),
  )
}

///|
pub fn MemQueue::new_with_runtime(
  registry : @taskq.Registry,
  options : @taskq.QueueOptions,
  runtime_options : @taskq.QueueRuntimeOptions,
) -> MemQueue {
  let created_at_ms = (runtime_options.clock.now_ms)()
  let initial_rate_tokens = match options.rate_limit_per_second {
    Some(rate_limit) if rate_limit > 0 => rate_limit.to_double()
    None => 0.0
    _ => 0.0
  }
  {
    name: options.queue_name,
    registry,
    options,
    hooks: runtime_options.hooks.copy(),
    clock: runtime_options.clock,
    plan: @runtime.phase_two_runtime_plan(options),
    lifecycle: @runtime.QueueLifecycle::Running,
    jobs: {},
    dedupe_index: {},
    ready_queue: [],
    delayed_queue: [],
    inflight: {},
    next_job_id: 1,
    processed: 0,
    succeeded: 0,
    retried: 0,
    failed: 0,
    dead: 0,
    paused: false,
    consecutive_failures: 0,
    rate_tokens: initial_rate_tokens,
    rate_last_refill_ms: created_at_ms,
    hook_errors: 0,
  }
}

///|
pub fn MemQueue::queue_name(self : MemQueue) -> String {
  self.name
}

///|
pub fn MemQueue::runtime_plan(self : MemQueue) -> @runtime.RuntimePlan {
  self.plan
}

///|
pub fn MemQueue::lifecycle(self : MemQueue) -> @runtime.QueueLifecycle {
  self.lifecycle
}

///|
pub fn MemQueue::job_count(self : MemQueue) -> Int {
  self.jobs.length()
}

///|
pub fn MemQueue::jobs_snapshot(self : MemQueue) -> Array[@taskq.JobEnvelope] {
  Array::from_iter(self.jobs.values())
}

///|
pub fn MemQueue::registered_tasks(self : MemQueue) -> Array[String] {
  self.registry.task_names()
}

///|
pub fn MemQueue::snapshot(self : MemQueue) -> @taskq.StatsSnapshot {
  match (try! apply_command(self, @runtime.PollStats)) {
    Snapshot(snapshot) => snapshot
    _ => snapshot_direct(self)
  }
}

///|
pub fn MemQueue::ready_job_count(self : MemQueue) -> Int {
  self.ready_queue.length()
}

///|
pub fn MemQueue::inflight_job_count(self : MemQueue) -> Int {
  self.inflight.length()
}

///|
fn build_job_id(queue : MemQueue, next_job_id : Int) -> String {
  "\{queue.name}-job-\{next_job_id}"
}

///|
fn build_context(
  queue : MemQueue,
  job : @taskq.JobEnvelope,
  started_at_ms : Int64,
) -> @taskq.TaskContext {
  {
    queue_name: queue.name,
    job_id: job.id,
    attempt: job.attempt,
    max_attempts: job.max_attempts,
    enqueued_at_ms: job.created_at_ms,
    started_at_ms,
    deadline_ms: job.deadline_ms,
    dedupe_key: job.dedupe_key,
  }
}

///|
fn maybe_finish_shutdown(queue : MemQueue) -> Unit {
  if queue.lifecycle == @runtime.QueueLifecycle::Closing &&
    queue.delayed_queue.is_empty() &&
    queue.ready_queue.is_empty() &&
    queue.inflight.is_empty() {
    queue.lifecycle = @runtime.QueueLifecycle::Closed
  }
}

///|
fn cleanup_dedupe_key(queue : MemQueue, job : @taskq.JobEnvelope) -> Unit {
  match job.dedupe_key {
    Some(dedupe_key) => queue.dedupe_index.remove(dedupe_key)
    None => ()
  }
}

///|
fn mark_attempt_succeeded(queue : MemQueue) -> Unit {
  queue.consecutive_failures = 0
}

///|
fn mark_attempt_failed(queue : MemQueue) -> Unit {
  queue.consecutive_failures = queue.consecutive_failures + 1
  if queue.options.pause_after_failures > 0 &&
    queue.consecutive_failures >= queue.options.pause_after_failures {
    queue.paused = true
  }
}

///|
fn refill_rate_tokens(queue : MemQueue, current_time_ms : Int64) -> Unit {
  match queue.options.rate_limit_per_second {
    Some(rate_limit) if rate_limit > 0 => {
      let elapsed_ms = current_time_ms - queue.rate_last_refill_ms
      if elapsed_ms > Int64::default() {
        let replenished = queue.rate_tokens +
          elapsed_ms.to_double() * rate_limit.to_double() / 1000.0
        queue.rate_tokens = replenished.min(rate_limit.to_double())
        queue.rate_last_refill_ms = current_time_ms
      }
    }
    None => ()
    _ => ()
  }
}

///|
fn try_consume_rate_token(queue : MemQueue, current_time_ms : Int64) -> Bool {
  match queue.options.rate_limit_per_second {
    Some(rate_limit) if rate_limit > 0 => {
      refill_rate_tokens(queue, current_time_ms)
      if queue.rate_tokens >= 1.0 {
        queue.rate_tokens = queue.rate_tokens - 1.0
        true
      } else {
        false
      }
    }
    None => true
    _ => true
  }
}

///|
fn next_rate_wait_ms(queue : MemQueue, current_time_ms : Int64) -> Int? {
  match queue.options.rate_limit_per_second {
    Some(rate_limit) if rate_limit > 0 => {
      refill_rate_tokens(queue, current_time_ms)
      if queue.rate_tokens >= 1.0 {
        None
      } else {
        let missing_tokens = 1.0 - queue.rate_tokens
        Some(
          Int::max(
            1,
            (missing_tokens * 1000.0 / rate_limit.to_double()).ceil().to_int(),
          ),
        )
      }
    }
    None => None
    _ => None
  }
}

///|
fn snapshot_direct(queue : MemQueue) -> @taskq.StatsSnapshot {
  {
    queued: queue.ready_queue.length(),
    delayed: queue.delayed_queue.length(),
    inflight: queue.inflight.length(),
    processed: queue.processed,
    succeeded: queue.succeeded,
    retried: queue.retried,
    failed: queue.failed,
    dead: queue.dead,
    active_workers: queue.inflight.length(),
    paused: queue.paused,
  }
}

///|
fn promote_due_jobs(queue : MemQueue, now_ms : Int64) -> Unit raise {
  fn promote_remaining(
    queue : MemQueue,
    now_ms : Int64,
    index : Int,
  ) -> Unit raise {
    guard index < queue.delayed_queue.length() else { return }
    let job_id = queue.delayed_queue[index]
    let job = match queue.jobs.get(job_id) {
      Some(job) => job
      None =>
        raise @taskq.QueueError::StateInvariant(
          "delayed queue referenced missing job \{job_id}",
        )
    }
    if job.visible_at_ms <= now_ms {
      ignore(queue.delayed_queue.remove(index))
      queue.ready_queue.push(job_id)
      promote_remaining(queue, now_ms, index)
    } else {
      promote_remaining(queue, now_ms, index + 1)
    }
  }
  promote_remaining(queue, now_ms, 0)
}

///|
fn next_delay_wait_ms(queue : MemQueue, now_ms : Int64) -> Int? raise {
  fn scan_next_due(
    queue : MemQueue,
    index : Int,
    now_ms : Int64,
    current_due_ms : Int64?,
  ) -> Int64? raise {
    guard index < queue.delayed_queue.length() else { return current_due_ms }
    let job_id = queue.delayed_queue[index]
    let job = match queue.jobs.get(job_id) {
      Some(job) => job
      None =>
        raise @taskq.QueueError::StateInvariant(
          "delayed queue referenced missing job \{job_id}",
        )
    }
    let next_due_ms = match current_due_ms {
      Some(current_due_ms) =>
        if job.visible_at_ms < current_due_ms {
          job.visible_at_ms
        } else {
          current_due_ms
        }
      None => job.visible_at_ms
    }
    scan_next_due(queue, index + 1, now_ms, Some(next_due_ms))
  }
  match scan_next_due(queue, 0, now_ms, None) {
    Some(due_ms) => Some(Int::max(0, (due_ms - now_ms).to_int()))
    None => None
  }
}

///|
fn apply_command(
  queue : MemQueue,
  command : @runtime.Command,
) -> CommandReply raise {
  match command {
    @runtime.Enqueue(job~) => {
      if !@runtime.is_accepting_commands(queue.lifecycle) {
        raise @taskq.QueueError::QueueClosed(queue.name)
      }
      match job.dedupe_key {
        Some(dedupe_key) =>
          if queue.dedupe_index.contains(dedupe_key) {
            raise @taskq.QueueError::DuplicateJob(dedupe_key)
          } else {
            queue.dedupe_index[dedupe_key] = job.id
          }
        None => ()
      }
      queue.jobs[job.id] = job
      if job.visible_at_ms <= clock_now_ms(queue) {
        queue.ready_queue.push(job.id)
      } else {
        queue.delayed_queue.push(job.id)
      }
      Done
    }
    @runtime.Dispatch =>
      if queue.paused || !@runtime.can_dispatch_work(queue.lifecycle) {
        maybe_finish_shutdown(queue)
        Lease(None)
      } else {
        let dispatch_time_ms = clock_now_ms(queue)
        promote_due_jobs(queue, dispatch_time_ms)
        let next_job_id = queue.ready_queue.get(0)
        guard next_job_id is Some(job_id) else {
          maybe_finish_shutdown(queue)
          return Lease(None)
        }
        if !try_consume_rate_token(queue, dispatch_time_ms) {
          return Lease(None)
        }
        ignore(queue.ready_queue.remove(0))
        let job = match queue.jobs.get(job_id) {
          Some(job) => job
          None =>
            raise @taskq.QueueError::StateInvariant(
              "ready queue referenced missing job \{job_id}",
            )
        }
        queue.inflight[job_id] = job
        Lease(
          Some({ job, context: build_context(queue, job, dispatch_time_ms) }),
        )
      }
    @runtime.Ack(job_id~) => {
      if !queue.inflight.contains(job_id) {
        raise @taskq.QueueError::StateInvariant(
          "complete_success missing inflight job \{job_id}",
        )
      }
      let job = match queue.jobs.get(job_id) {
        Some(job) => job
        None =>
          raise @taskq.QueueError::StateInvariant(
            "complete_success missing job \{job_id}",
          )
      }
      queue.inflight.remove(job_id)
      queue.jobs.remove(job_id)
      cleanup_dedupe_key(queue, job)
      queue.processed = queue.processed + 1
      queue.succeeded = queue.succeeded + 1
      mark_attempt_succeeded(queue)
      maybe_finish_shutdown(queue)
      Done
    }
    @runtime.Nack(job_id~, error_message~) => {
      if !queue.inflight.contains(job_id) || !queue.jobs.contains(job_id) {
        raise @taskq.QueueError::StateInvariant(
          "complete_failure missing job \{job_id}",
        )
      }
      let job = match queue.jobs.get(job_id) {
        Some(job) => job
        None =>
          raise @taskq.QueueError::StateInvariant(
            "complete_failure missing job \{job_id}",
          )
      }
      let policy = @taskq.registry_retry_policy(queue.registry, job.task_name)
      queue.inflight.remove(job_id)
      queue.processed = queue.processed + 1
      mark_attempt_failed(queue)
      match next_retry_visible_at_ms(queue.clock, job, policy) {
        Some(next_visible_at_ms) if job.attempt < job.max_attempts => {
          let retried_job : @taskq.JobEnvelope = {
            id: job.id,
            task_name: job.task_name,
            payload: job.payload,
            dedupe_key: job.dedupe_key,
            created_at_ms: job.created_at_ms,
            visible_at_ms: next_visible_at_ms,
            deadline_ms: job.deadline_ms,
            attempt: job.attempt + 1,
            max_attempts: job.max_attempts,
            last_error: Some(error_message),
          }
          queue.jobs[job_id] = retried_job
          if next_visible_at_ms <= clock_now_ms(queue) {
            queue.ready_queue.push(job_id)
          } else {
            queue.delayed_queue.push(job_id)
          }
          queue.retried = queue.retried + 1
          maybe_finish_shutdown(queue)
          Failure(Retried)
        }
        _ => {
          queue.jobs.remove(job_id)
          cleanup_dedupe_key(queue, job)
          queue.failed = queue.failed + 1
          queue.dead = queue.dead + 1
          maybe_finish_shutdown(queue)
          Failure(Dead)
        }
      }
    }
    @runtime.PollStats => Snapshot(snapshot_direct(queue))
    @runtime.Shutdown(graceful~, timeout_ms~) => {
      ignore(timeout_ms)
      queue.lifecycle = if graceful {
        @runtime.QueueLifecycle::Closing
      } else {
        @runtime.QueueLifecycle::Closed
      }
      maybe_finish_shutdown(queue)
      Done
    }
    @runtime.Pause(reason~) => {
      ignore(reason)
      queue.paused = true
      Done
    }
    @runtime.Resume => {
      queue.paused = false
      queue.consecutive_failures = 0
      Done
    }
    @runtime.Cancel(job_id~, reason~) => {
      ignore(job_id)
      ignore(reason)
      raise @taskq.QueueError::InvalidQueueOptions(
        "phase two memqueue does not implement cancel yet",
      )
    }
  }
}

///|
fn drain_ready_leases(queue : MemQueue) -> Array[WorkerLease] raise {
  fn drain_remaining(
    queue : MemQueue,
    leases : Array[WorkerLease],
  ) -> Array[WorkerLease] raise {
    let next_lease = queue.dispatch_next()
    match next_lease {
      Some(lease) => {
        leases.push(lease)
        drain_remaining(queue, leases)
      }
      None => leases
    }
  }
  drain_remaining(queue, [])
}

///|
fn nack_lease(
  queue : MemQueue,
  lease : WorkerLease,
  error_message : String,
) -> FailureAction raise {
  match
    apply_command(queue, @runtime.Nack(job_id=lease.job.id, error_message~)) {
    Failure(action) => action
    _ =>
      raise @taskq.QueueError::StateInvariant(
        "nack must return a failure action",
      )
  }
}

///|
pub fn[A] MemQueue::enqueue(
  self : MemQueue,
  task : @taskq.Task[A],
  payload : A,
  options : @taskq.EnqueueOptions,
) -> @taskq.JobId raise {
  let enqueue_time_ms = clock_now_ms(self)
  let payload_bytes = (task.codec.encode)(payload)
  let job_id = build_job_id(self, self.next_job_id)
  let next_visible_at = visible_at_ms_from(enqueue_time_ms, options)
  let job : @taskq.JobEnvelope = {
    id: job_id,
    task_name: task.name,
    payload: payload_bytes,
    dedupe_key: options.dedupe_key,
    created_at_ms: enqueue_time_ms,
    visible_at_ms: next_visible_at,
    deadline_ms: deadline_ms_from(enqueue_time_ms, options),
    attempt: 1,
    max_attempts: @taskq.max_attempts_for_task(
      task,
      options.max_attempts_override,
    ),
    last_error: None,
  }
  ignore(apply_command(self, @runtime.Enqueue(job~)))
  self.next_job_id = self.next_job_id + 1
  job_id
}

///|
pub fn MemQueue::dispatch_next(self : MemQueue) -> WorkerLease? raise {
  match apply_command(self, @runtime.Dispatch) {
    Lease(lease) => lease
    _ =>
      raise @taskq.QueueError::StateInvariant(
        "dispatch must return a lease reply",
      )
  }
}

///|
pub fn MemQueue::complete_success(
  self : MemQueue,
  lease : WorkerLease,
) -> Unit raise {
  ignore(apply_command(self, @runtime.Ack(job_id=lease.job.id)))
}

///|
pub fn MemQueue::complete_failure(
  self : MemQueue,
  lease : WorkerLease,
  error_message : String,
) -> Unit raise {
  ignore(nack_lease(self, lease, error_message))
}

///|
pub fn MemQueue::pause(self : MemQueue, reason : String) -> Unit raise {
  ignore(apply_command(self, @runtime.Pause(reason~)))
}

///|
pub fn MemQueue::resume_consuming(self : MemQueue) -> Unit raise {
  ignore(apply_command(self, @runtime.Resume))
}

///|
pub async fn MemQueue::process_next(self : MemQueue) -> @taskq.JobId? {
  let lease = self.dispatch_next()
  guard lease is Some(lease) else { return None }
  let event = build_process_event(lease)
  run_before_hooks(self, event)
  let result = run_job_attempt(
    self.registry,
    self.clock,
    lease.context,
    lease.job,
  )
  match result {
    Ok(_) => {
      self.complete_success(lease)
      let finished_at_ms = clock_now_ms(self)
      run_after_hooks(
        self,
        event,
        build_process_result(
          @taskq.Succeeded,
          None,
          event.started_at_ms,
          finished_at_ms,
          None,
        ),
      )
    }
    Err(err) => {
      let action = try! nack_lease(self, lease, err.to_string())
      let finished_at_ms = clock_now_ms(self)
      let outcome = match action {
        Retried => @taskq.FailedWillRetry
        Dead => @taskq.FailedDead
      }
      let next_visible_at_ms = match action {
        Retried =>
          match self.jobs.get(lease.job.id) {
            Some(job) => Some(job.visible_at_ms)
            None => None
          }
        Dead => None
      }
      run_after_hooks(
        self,
        event,
        build_process_result(
          outcome,
          Some(err.to_string()),
          event.started_at_ms,
          finished_at_ms,
          next_visible_at_ms,
        ),
      )
      if action is Dead {
        run_job_fallback(
          self.registry,
          lease.context,
          lease.job,
          err.to_string(),
        )
      }
    }
  }
  Some(lease.job.id)
}

///|
pub async fn MemQueue::process_all(self : MemQueue) -> Int {
  async fn process_remaining(queue : MemQueue, processed_count : Int) -> Int {
    let leases = drain_ready_leases(queue)
    if !leases.is_empty() {
      let workers : Array[async () -> WorkerOutcome] = leases.map(fn(lease) {
        async fn() {
          let event = build_process_event(lease)
          run_before_hooks(queue, event)
          let result = run_job_attempt(
            queue.registry,
            queue.clock,
            lease.context,
            lease.job,
          )
          { lease, result }
        }
      })
      let outcomes = @async.all(
        workers[:],
        max_concurrent=queue.options.max_concurrency,
      )
      for outcome in outcomes {
        let event = build_process_event(outcome.lease)
        match outcome.result {
          Ok(_) => {
            try! queue.complete_success(outcome.lease)
            let finished_at_ms = clock_now_ms(queue)
            run_after_hooks(
              queue,
              event,
              build_process_result(
                @taskq.Succeeded,
                None,
                event.started_at_ms,
                finished_at_ms,
                None,
              ),
            )
          }
          Err(err) => {
            let action = try! nack_lease(queue, outcome.lease, err.to_string())
            let finished_at_ms = clock_now_ms(queue)
            let result = match action {
              Retried =>
                build_process_result(
                  @taskq.FailedWillRetry,
                  Some(err.to_string()),
                  event.started_at_ms,
                  finished_at_ms,
                  match queue.jobs.get(outcome.lease.job.id) {
                    Some(job) => Some(job.visible_at_ms)
                    None => None
                  },
                )
              Dead =>
                build_process_result(
                  @taskq.FailedDead,
                  Some(err.to_string()),
                  event.started_at_ms,
                  finished_at_ms,
                  None,
                )
            }
            run_after_hooks(queue, event, result)
            if action is Dead {
              run_job_fallback(
                queue.registry,
                outcome.lease.context,
                outcome.lease.job,
                err.to_string(),
              )
            }
          }
        }
      }
      process_remaining(queue, processed_count + leases.length())
    } else {
      let current_time_ms = clock_now_ms(queue)
      if !queue.ready_queue.is_empty() {
        match next_rate_wait_ms(queue, current_time_ms) {
          Some(wait_ms) =>
            if wait_ms > 0 &&
              !queue.paused &&
              @runtime.can_dispatch_work(queue.lifecycle) {
              sleep_with_clock(queue, wait_ms)
              process_remaining(queue, processed_count)
            } else {
              maybe_finish_shutdown(queue)
              processed_count
            }
          _ => {
            maybe_finish_shutdown(queue)
            processed_count
          }
        }
      } else {
        match next_delay_wait_ms(queue, current_time_ms) {
          Some(wait_ms) =>
            if wait_ms > 0 &&
              !queue.paused &&
              @runtime.can_dispatch_work(queue.lifecycle) {
              sleep_with_clock(queue, wait_ms)
              process_remaining(queue, processed_count)
            } else {
              maybe_finish_shutdown(queue)
              processed_count
            }
          _ => {
            maybe_finish_shutdown(queue)
            processed_count
          }
        }
      }
    }
  }
  process_remaining(self, 0)
}

///|
pub async fn MemQueue::shutdown_gracefully(
  self : MemQueue,
  timeout_ms? : Int,
) -> Unit {
  let shutdown_timeout_ms = match timeout_ms {
    Some(timeout_ms) => timeout_ms
    None => 0
  }
  ignore(
    apply_command(
      self,
      @runtime.Shutdown(graceful=true, timeout_ms=shutdown_timeout_ms),
    ),
  )
  if self.inflight.is_empty() {
    ignore(self.process_all())
  } else {
    maybe_finish_shutdown(self)
  }
}

///|
pub fn MemQueue::close_now(self : MemQueue) -> Unit {
  ignore(
    try! apply_command(self, @runtime.Shutdown(graceful=false, timeout_ms=0)),
  )
}
