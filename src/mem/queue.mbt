///|

///|
pub(all) struct WorkerLease {
  job : @taskq.JobEnvelope
  context : @taskq.TaskContext
} derive(Eq, ToJson, Debug)

///|
pub struct MemQueue {
  name : String
  registry : @taskq.Registry
  options : @taskq.QueueOptions
  plan : @runtime.RuntimePlan
  mut lifecycle : @runtime.QueueLifecycle
  jobs : Map[String, @taskq.JobEnvelope]
  ready_queue : Array[@taskq.JobId]
  inflight : Map[String, @taskq.JobEnvelope]
  mut next_job_id : Int
  mut processed : Int
  mut succeeded : Int
  retried : Int
  mut failed : Int
  mut dead : Int
  paused : Bool
}

///|
pub fn MemQueue::new(
  registry : @taskq.Registry,
  options : @taskq.QueueOptions,
) -> MemQueue {
  {
    name: options.queue_name,
    registry,
    options,
    plan: @runtime.phase_two_runtime_plan(options),
    lifecycle: @runtime.QueueLifecycle::Running,
    jobs: {},
    ready_queue: [],
    inflight: {},
    next_job_id: 1,
    processed: 0,
    succeeded: 0,
    retried: 0,
    failed: 0,
    dead: 0,
    paused: false,
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
  {
    queued: self.ready_queue.length(),
    delayed: 0,
    inflight: self.inflight.length(),
    processed: self.processed,
    succeeded: self.succeeded,
    retried: self.retried,
    failed: self.failed,
    dead: self.dead,
    active_workers: self.inflight.length(),
    paused: self.paused,
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
) -> @taskq.TaskContext {
  {
    queue_name: queue.name,
    job_id: job.id,
    attempt: job.attempt,
    max_attempts: job.max_attempts,
    enqueued_at_ms: job.created_at_ms,
    started_at_ms: Int64::default(),
    deadline_ms: job.deadline_ms,
    dedupe_key: job.dedupe_key,
  }
}

///|
fn maybe_finish_shutdown(queue : MemQueue) -> Unit {
  if queue.lifecycle == @runtime.QueueLifecycle::Closing &&
    queue.ready_queue.is_empty() &&
    queue.inflight.is_empty() {
    queue.lifecycle = @runtime.QueueLifecycle::Closed
  }
}

///|
fn visible_at(options : @taskq.EnqueueOptions) -> Int64 {
  match options.schedule_at_ms {
    Some(at_ms) => at_ms
    None => Int64::from_int(options.delay_ms)
  }
}

///|
fn deadline_ms(options : @taskq.EnqueueOptions) -> Int64? {
  match options.timeout_ms {
    Some(timeout_ms) => Some(Int64::from_int(timeout_ms))
    None => None
  }
}

///|
pub fn[A] MemQueue::enqueue(
  self : MemQueue,
  task : @taskq.Task[A],
  payload : A,
  options : @taskq.EnqueueOptions,
) -> @taskq.JobId raise {
  if !@runtime.is_accepting_commands(self.lifecycle) {
    raise @taskq.QueueError::QueueClosed(self.name)
  }
  let payload_bytes = (task.codec.encode)(payload)
  let job_id = build_job_id(self, self.next_job_id)
  self.next_job_id = self.next_job_id + 1
  let next_visible_at = visible_at(options)
  let job : @taskq.JobEnvelope = {
    id: job_id,
    task_name: task.name,
    payload: payload_bytes,
    dedupe_key: options.dedupe_key,
    created_at_ms: Int64::default(),
    visible_at_ms: next_visible_at,
    deadline_ms: deadline_ms(options),
    attempt: 1,
    max_attempts: @taskq.max_attempts_for_task(
      task,
      options.max_attempts_override,
    ),
    last_error: None,
  }
  self.jobs[job_id] = job
  if next_visible_at == Int64::default() {
    self.ready_queue.push(job_id)
  } else {
    raise @taskq.QueueError::InvalidQueueOptions(
      "phase two memqueue only accepts ready jobs; delayed scheduling belongs to M3",
    )
  }
  job_id
}

///|
pub fn MemQueue::dispatch_next(self : MemQueue) -> WorkerLease? raise {
  if !@runtime.can_dispatch_work(self.lifecycle) {
    return None
  }
  let next_job_id = self.ready_queue.get(0)
  guard next_job_id is Some(job_id) else {
    maybe_finish_shutdown(self)
    return None
  }
  ignore(self.ready_queue.remove(0))
  let job = match self.jobs.get(job_id) {
    Some(job) => job
    None =>
      raise @taskq.QueueError::StateInvariant(
        "ready queue referenced missing job \{job_id}",
      )
  }
  self.inflight[job_id] = job
  Some({ job, context: build_context(self, job) })
}

///|
pub fn MemQueue::complete_success(
  self : MemQueue,
  lease : WorkerLease,
) -> Unit raise {
  let job_id = lease.job.id
  if !self.inflight.contains(job_id) {
    raise @taskq.QueueError::StateInvariant(
      "complete_success missing inflight job \{job_id}",
    )
  }
  self.inflight.remove(job_id)
  self.jobs.remove(job_id)
  self.processed = self.processed + 1
  self.succeeded = self.succeeded + 1
  maybe_finish_shutdown(self)
}

///|
pub fn MemQueue::complete_failure(
  self : MemQueue,
  lease : WorkerLease,
  error_message : String,
) -> Unit raise {
  let job_id = lease.job.id
  if !self.jobs.contains(job_id) {
    raise @taskq.QueueError::StateInvariant(
      "complete_failure missing job \{job_id}",
    )
  }
  self.inflight.remove(job_id)
  self.jobs.remove(job_id)
  ignore(error_message)
  self.processed = self.processed + 1
  self.failed = self.failed + 1
  self.dead = self.dead + 1
  maybe_finish_shutdown(self)
}

///|
pub async fn MemQueue::process_next(self : MemQueue) -> @taskq.JobId? {
  let lease = self.dispatch_next()
  guard lease is Some(lease) else { return None }
  let result : Result[Unit, Error] = try? @taskq.run_registered_job(
    self.registry,
    lease.context,
    lease.job,
  )
  match result {
    Ok(_) => self.complete_success(lease)
    Err(err) => self.complete_failure(lease, err.to_string())
  }
  Some(lease.job.id)
}

///|
pub async fn MemQueue::process_all(self : MemQueue) -> Int {
  async fn process_remaining(queue : MemQueue, processed_count : Int) -> Int {
    let next_job_id = queue.process_next()
    match next_job_id {
      Some(_) => process_remaining(queue, processed_count + 1)
      None => processed_count
    }
  }
  let drained_count = process_remaining(self, 0)
  maybe_finish_shutdown(self)
  drained_count
}

///|
pub async fn MemQueue::shutdown_gracefully(
  self : MemQueue,
  timeout_ms? : Int,
) -> Unit {
  ignore(timeout_ms)
  self.lifecycle = @runtime.QueueLifecycle::Closing
  if self.inflight.is_empty() {
    ignore(self.process_all())
  } else {
    maybe_finish_shutdown(self)
  }
}

///|
pub fn MemQueue::close_now(self : MemQueue) -> Unit {
  self.lifecycle = @runtime.QueueLifecycle::Closed
}
