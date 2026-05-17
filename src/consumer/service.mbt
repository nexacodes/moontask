///|
/// Lifecycle state of a resident consumer service.
pub(all) enum ConsumerLifecycle {
  Idle
  Starting
  Running
  Stopping
  Stopped
} derive(Eq, ToJson, Debug)

///|
/// Static configuration for reserving and processing jobs from one queue.
pub(all) struct ConsumerOptions {
  queue_name : String
  consumer_id : String
  max_concurrency : Int
  reserve_batch_size : Int
  lease_ms : Int
  poll_interval_ms : Int
  pause_poll_interval_ms : Int
  heartbeat_interval_ms : Int
  reaper_interval_ms : Int
} derive(Eq, ToJson, Debug)

///|
/// Runtime dependencies used by `ConsumerService`.
pub(all) struct ConsumerRuntimeOptions {
  hooks : Array[&@core.Hook]
  clock : @core.RuntimeClock
}

///|
/// Lightweight runtime snapshot for observability and admin APIs.
pub(all) struct ConsumerSnapshot {
  queue_name : String
  consumer_id : String
  lifecycle : ConsumerLifecycle
  inflight_count : Int
  available_worker_slots : Int
  started_at_ms : Int64?
  stopped_at_ms : Int64?
} derive(Eq, ToJson, Debug)

///|
/// Detailed inspection snapshot including in-flight lease state.
pub(all) struct ConsumerInspection {
  snapshot : ConsumerSnapshot
  accepting_work : Bool
  inflight_leases : Array[@core.Lease]
  pending_ack_count : Int
  stop_requested_at_ms : Int64?
  stop_deadline_ms : Int64?
} derive(Eq, ToJson, Debug)

///|
/// Runtime counters accumulated while a consumer is running.
pub(all) struct ConsumerRuntimeMetrics {
  reserve_calls : Int
  reserved_jobs : Int
  reserve_latency_ms_total : Int64
  ack_calls : Int
  ack_latency_ms_total : Int64
  nack_retry_calls : Int
  nack_dead_calls : Int
  lease_extensions : Int
  expired_leases_reaped : Int
  processed_attempts : Int
  succeeded_attempts : Int
  failed_retry_attempts : Int
  failed_dead_attempts : Int
  cancelled_attempts : Int
} derive(Eq, ToJson, Debug)

///|
/// Resident queue consumer that reserves leases and dispatches task handlers.
pub struct ConsumerService[B] {
  backend : B
  registry : @registry.Registry
  options : ConsumerOptions
  hooks : Array[&@core.Hook]
  clock : @core.RuntimeClock
  worker_semaphore : @async.Semaphore
  inflight_leases : Map[String, @core.Lease]
  mut result_queue : @async.Queue[AttemptResult]?
  mut lifecycle : ConsumerLifecycle
  mut inflight_count : Int
  mut pending_ack_count : Int
  mut started_at_ms : Int64?
  mut stopped_at_ms : Int64?
  mut stop_requested_at_ms : Int64?
  mut stop_deadline_ms : Int64?
  mut last_heartbeat_at_ms : Int64?
  mut last_reaper_at_ms : Int64?
  mut sleep_interrupt : (() -> Unit)?
  mut reserve_calls : Int
  mut reserved_jobs : Int
  mut reserve_latency_ms_total : Int64
  mut ack_calls : Int
  mut ack_latency_ms_total : Int64
  mut nack_retry_calls : Int
  mut nack_dead_calls : Int
  mut lease_extensions : Int
  mut expired_leases_reaped : Int
  mut processed_attempts : Int
  mut succeeded_attempts : Int
  mut failed_retry_attempts : Int
  mut failed_dead_attempts : Int
  mut cancelled_attempts : Int
}

///|
/// Returns conservative defaults suitable for a single-worker consumer.
pub fn default_consumer_options(
  queue_name : String,
  consumer_id : String,
) -> ConsumerOptions {
  {
    queue_name,
    consumer_id,
    max_concurrency: 1,
    reserve_batch_size: 1,
    lease_ms: 30_000,
    poll_interval_ms: 1000,
    pause_poll_interval_ms: 3000,
    heartbeat_interval_ms: 1000,
    reaper_interval_ms: 1000,
  }
}

///|
/// Returns consumer runtime defaults backed by the system clock and no hooks.
pub fn default_consumer_runtime_options() -> ConsumerRuntimeOptions {
  { hooks: [], clock: @core.system_runtime_clock() }
}

///|
/// Creates a consumer with default runtime dependencies.
pub fn[B] ConsumerService::new(
  backend : B,
  registry : @registry.Registry,
  options : ConsumerOptions,
) -> ConsumerService[B] {
  ConsumerService::new_with_runtime(
    backend,
    registry,
    options,
    default_consumer_runtime_options(),
  )
}

///|
/// Creates a consumer with explicit runtime dependencies.
pub fn[B] ConsumerService::new_with_runtime(
  backend : B,
  registry : @registry.Registry,
  options : ConsumerOptions,
  runtime_options : ConsumerRuntimeOptions,
) -> ConsumerService[B] {
  {
    backend,
    registry,
    options,
    hooks: runtime_options.hooks.copy(),
    clock: runtime_options.clock,
    worker_semaphore: @async.Semaphore::new(options.max_concurrency),
    inflight_leases: {},
    result_queue: None,
    lifecycle: Idle,
    inflight_count: 0,
    pending_ack_count: 0,
    started_at_ms: None,
    stopped_at_ms: None,
    stop_requested_at_ms: None,
    stop_deadline_ms: None,
    last_heartbeat_at_ms: None,
    last_reaper_at_ms: None,
    sleep_interrupt: None,
    reserve_calls: 0,
    reserved_jobs: 0,
    reserve_latency_ms_total: 0L,
    ack_calls: 0,
    ack_latency_ms_total: 0L,
    nack_retry_calls: 0,
    nack_dead_calls: 0,
    lease_extensions: 0,
    expired_leases_reaped: 0,
    processed_attempts: 0,
    succeeded_attempts: 0,
    failed_retry_attempts: 0,
    failed_dead_attempts: 0,
    cancelled_attempts: 0,
  }
}

///|
/// Returns the queue name this consumer reads from.
pub fn[B] ConsumerService::queue_name(self : ConsumerService[B]) -> String {
  self.options.queue_name
}

///|
/// Returns the stable identifier used when reserving leases.
pub fn[B] ConsumerService::consumer_id(self : ConsumerService[B]) -> String {
  self.options.consumer_id
}

///|
/// Returns the task registry used to decode and dispatch jobs.
pub fn[B] ConsumerService::registry(
  self : ConsumerService[B],
) -> @registry.Registry {
  self.registry
}

///|
/// Returns the current lifecycle state of the resident runtime.
pub fn[B] ConsumerService::lifecycle(
  self : ConsumerService[B],
) -> ConsumerLifecycle {
  self.lifecycle
}

///|
/// Returns a lightweight snapshot of the current runtime state.
pub fn[B] ConsumerService::snapshot(
  self : ConsumerService[B],
) -> ConsumerSnapshot {
  {
    queue_name: self.options.queue_name,
    consumer_id: self.options.consumer_id,
    lifecycle: self.lifecycle,
    inflight_count: self.inflight_count,
    available_worker_slots: self.available_worker_slots(),
    started_at_ms: self.started_at_ms,
    stopped_at_ms: self.stopped_at_ms,
  }
}

///|
/// Returns when graceful shutdown was requested, if any.
pub fn[B] ConsumerService::stop_requested_at_ms(
  self : ConsumerService[B],
) -> Int64? {
  self.stop_requested_at_ms
}

///|
/// Returns the forced-stop deadline computed from `stop(timeout_ms)`.
pub fn[B] ConsumerService::stop_deadline_ms(
  self : ConsumerService[B],
) -> Int64? {
  self.stop_deadline_ms
}

///|
/// Returns a detailed inspection snapshot including in-flight leases.
pub fn[B] ConsumerService::inspect(
  self : ConsumerService[B],
) -> ConsumerInspection {
  {
    snapshot: self.snapshot(),
    accepting_work: self.is_accepting_work(),
    inflight_leases: inflight_leases_snapshot(self),
    pending_ack_count: self.pending_ack_count,
    stop_requested_at_ms: self.stop_requested_at_ms,
    stop_deadline_ms: self.stop_deadline_ms,
  }
}

///|
/// Returns cumulative runtime counters for the current or last run.
pub fn[B] ConsumerService::runtime_metrics(
  self : ConsumerService[B],
) -> ConsumerRuntimeMetrics {
  {
    reserve_calls: self.reserve_calls,
    reserved_jobs: self.reserved_jobs,
    reserve_latency_ms_total: self.reserve_latency_ms_total,
    ack_calls: self.ack_calls,
    ack_latency_ms_total: self.ack_latency_ms_total,
    nack_retry_calls: self.nack_retry_calls,
    nack_dead_calls: self.nack_dead_calls,
    lease_extensions: self.lease_extensions,
    expired_leases_reaped: self.expired_leases_reaped,
    processed_attempts: self.processed_attempts,
    succeeded_attempts: self.succeeded_attempts,
    failed_retry_attempts: self.failed_retry_attempts,
    failed_dead_attempts: self.failed_dead_attempts,
    cancelled_attempts: self.cancelled_attempts,
  }
}

///|
/// Starts the resident consumer loop and runs until stopped or cancelled.
pub async fn[B : @backend.QueueBackend] ConsumerService::start(
  self : ConsumerService[B],
) -> Unit {
  match self.lifecycle {
    Idle | Stopped => {
      self.lifecycle = Starting
      self.started_at_ms = Some((self.clock.now_ms)())
      self.stopped_at_ms = None
      self.stop_requested_at_ms = None
      self.stop_deadline_ms = None
      self.last_heartbeat_at_ms = None
      self.last_reaper_at_ms = None
      self.pending_ack_count = 0
      self.reset_runtime_metrics()
      self.result_queue = Some(@async.Queue::new(kind=Unbounded))
      self.lifecycle = Running
      @async.with_task_group(async fn(group) {
        let ack_task = group.spawn(
          async fn() { self.run_ack_loop() },
          allow_failure=true,
        )
        defer {
          self.close_result_queue()
          self.result_queue = None
          self.sleep_interrupt = None
        }
        while self.should_continue_runtime() {
          try! self.run_heartbeat_cycle()
          try! self.run_reaper_cycle()
          if self.lifecycle == Running || self.lifecycle == Starting {
            let leases = try! self.reserve_once()
            for lease in leases {
              ignore(
                group.spawn(
                  async fn() { process_lease(self, lease) },
                  no_wait=true,
                  allow_failure=true,
                ),
              )
            }
          }
          guard self.should_continue_runtime() else { break }
          self.sleep_until_next_poll()
        }
        self.close_result_queue()
        ignore(ack_task.wait())
      })
      reset_inflight_tracking(self)
      self.pending_ack_count = 0
      self.lifecycle = Stopped
      self.stopped_at_ms = Some((self.clock.now_ms)())
    }
    Starting | Running => ()
    Stopping =>
      raise @core.QueueError::StateInvariant(
        "consumer is stopping and cannot be started",
      )
  }
}

///|
/// Requests a graceful stop and forces shutdown after `timeout_ms` if needed.
pub fn[B] ConsumerService::stop(
  self : ConsumerService[B],
  timeout_ms : Int,
) -> Unit {
  match self.lifecycle {
    Running | Starting => {
      self.stop_requested_at_ms = Some((self.clock.now_ms)())
      self.stop_deadline_ms = Some(stop_deadline_from_timeout(self, timeout_ms))
      self.lifecycle = Stopping
      self.wake_sleeping_runtime()
    }
    Idle => {
      self.stop_requested_at_ms = Some((self.clock.now_ms)())
      self.stop_deadline_ms = Some(stop_deadline_from_timeout(self, timeout_ms))
      self.stopped_at_ms = Some((self.clock.now_ms)())
      self.lifecycle = Stopped
    }
    Stopping | Stopped => ()
  }
}
