///|
pub(all) enum ConsumerLifecycle {
  Idle
  Starting
  Running
  Stopping
  Stopped
} derive(Eq, ToJson, Debug)

///|
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
pub(all) struct ConsumerRuntimeOptions {
  hooks : Array[&@core.Hook]
  clock : @core.RuntimeClock
}

///|
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
pub(all) struct ConsumerInspection {
  snapshot : ConsumerSnapshot
  accepting_work : Bool
  inflight_leases : Array[@core.Lease]
  stop_requested_at_ms : Int64?
  stop_deadline_ms : Int64?
} derive(Eq, ToJson, Debug)

///|
pub struct ConsumerService[B] {
  backend : B
  registry : @registry.Registry
  options : ConsumerOptions
  hooks : Array[&@core.Hook]
  clock : @core.RuntimeClock
  worker_semaphore : @async.Semaphore
  inflight_leases : Map[String, @core.Lease]
  mut lifecycle : ConsumerLifecycle
  mut inflight_count : Int
  mut started_at_ms : Int64?
  mut stopped_at_ms : Int64?
  mut stop_requested_at_ms : Int64?
  mut stop_deadline_ms : Int64?
  mut last_heartbeat_at_ms : Int64?
  mut last_reaper_at_ms : Int64?
  mut runtime_shutdown : (() -> Unit)?
  mut sleep_interrupt : (() -> Unit)?
}

///|
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
pub fn default_consumer_runtime_options() -> ConsumerRuntimeOptions {
  { hooks: [], clock: @core.system_runtime_clock() }
}

///|
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
    lifecycle: Idle,
    inflight_count: 0,
    started_at_ms: None,
    stopped_at_ms: None,
    stop_requested_at_ms: None,
    stop_deadline_ms: None,
    last_heartbeat_at_ms: None,
    last_reaper_at_ms: None,
    runtime_shutdown: None,
    sleep_interrupt: None,
  }
}

///|
pub fn[B] ConsumerService::queue_name(self : ConsumerService[B]) -> String {
  self.options.queue_name
}

///|
pub fn[B] ConsumerService::consumer_id(self : ConsumerService[B]) -> String {
  self.options.consumer_id
}

///|
pub fn[B] ConsumerService::registry(
  self : ConsumerService[B],
) -> @registry.Registry {
  self.registry
}

///|
pub fn[B] ConsumerService::lifecycle(
  self : ConsumerService[B],
) -> ConsumerLifecycle {
  self.lifecycle
}

///|
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
pub fn[B] ConsumerService::stop_requested_at_ms(
  self : ConsumerService[B],
) -> Int64? {
  self.stop_requested_at_ms
}

///|
pub fn[B] ConsumerService::stop_deadline_ms(
  self : ConsumerService[B],
) -> Int64? {
  self.stop_deadline_ms
}

///|
pub fn[B] ConsumerService::inspect(
  self : ConsumerService[B],
) -> ConsumerInspection {
  {
    snapshot: self.snapshot(),
    accepting_work: self.is_accepting_work(),
    inflight_leases: inflight_leases_snapshot(self),
    stop_requested_at_ms: self.stop_requested_at_ms,
    stop_deadline_ms: self.stop_deadline_ms,
  }
}

///|
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
      self.lifecycle = Running
      @async.with_task_group(async fn(group) {
        self.runtime_shutdown = Some(fn() {
          ignore(try? group.return_immediately(()))
        })
        defer {
          self.runtime_shutdown = None
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
      })
      reset_inflight_tracking(self)
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
      if self.should_force_shutdown() {
        self.force_runtime_shutdown()
      }
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
