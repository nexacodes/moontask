///|
pub(all) struct ProducerRuntimeOptions {
  clock : @core.RuntimeClock
}

///|
pub struct Producer[B] {
  backend : B
  queue_name : String
  clock : @core.RuntimeClock
  mut next_job_sequence : Int
}

///|
pub fn default_producer_runtime_options() -> ProducerRuntimeOptions {
  { clock: @core.system_runtime_clock() }
}

///|
pub fn[B] Producer::new(backend : B, queue_name : String) -> Producer[B] {
  Producer::new_with_runtime(
    backend,
    queue_name,
    default_producer_runtime_options(),
  )
}

///|
pub fn[B] Producer::new_with_runtime(
  backend : B,
  queue_name : String,
  runtime_options : ProducerRuntimeOptions,
) -> Producer[B] {
  { backend, queue_name, clock: runtime_options.clock, next_job_sequence: 1 }
}

///|
pub fn[B] Producer::queue_name(self : Producer[B]) -> String {
  self.queue_name
}

///|
fn[B] build_job_id(producer : Producer[B], now_ms : Int64) -> String {
  let job_id = "job-\{now_ms}-\{producer.next_job_sequence}"
  producer.next_job_sequence = producer.next_job_sequence + 1
  job_id
}

///|
fn visible_at_ms_from_options(
  now_ms : Int64,
  options : @core.EnqueueOptions,
) -> Int64 {
  match options.schedule_at_ms {
    Some(schedule_at_ms) => schedule_at_ms
    None => now_ms + Int64::from_int(Int::max(0, options.delay_ms))
  }
}

///|
fn[A] build_job_record(
  task : @core.Task[A],
  queue_name : String,
  job_id : String,
  payload : A,
  options : @core.EnqueueOptions,
  now_ms : Int64,
) -> @core.JobRecord raise {
  {
    job_id,
    queue_name,
    task_name: task.name,
    payload: (task.codec.encode)(payload),
    status: @core.Pending,
    attempt: 1,
    max_attempts: @registry.max_attempts_for_task(
      task,
      options.max_attempts_override,
    ),
    dedupe_key: options.dedupe_key,
    created_at_ms: now_ms,
    visible_at_ms: visible_at_ms_from_options(now_ms, options),
    lease_id: None,
    lease_owner: None,
    lease_deadline_ms: None,
    started_at_ms: None,
    finished_at_ms: None,
    timeout_ms: options.timeout_ms,
    deadline_ms: None,
    last_error: None,
    trace_id: Some("trace-\{job_id}"),
  }
}

///|
async fn[B : @backend.QueueBackend, A] enqueue_internal(
  producer : Producer[B],
  task : @core.Task[A],
  payload : A,
  options : @core.EnqueueOptions,
) -> @core.JobId {
  let now_ms = (producer.clock.now_ms)()
  let job_id = build_job_id(producer, now_ms)
  let job = build_job_record(
    task,
    producer.queue_name,
    job_id,
    payload,
    options,
    now_ms,
  )
  producer.backend.enqueue(job)
}

///|
pub async fn[B : @backend.QueueBackend, A] Producer::enqueue(
  self : Producer[B],
  task : @core.Task[A],
  payload : A,
) -> @core.JobId {
  enqueue_internal(self, task, payload, @core.default_enqueue_options())
}

///|
pub async fn[B : @backend.QueueBackend, A] Producer::enqueue_with_options(
  self : Producer[B],
  task : @core.Task[A],
  payload : A,
  options : @core.EnqueueOptions,
) -> @core.JobId {
  enqueue_internal(self, task, payload, options)
}
