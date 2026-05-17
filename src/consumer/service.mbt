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
  mut last_heartbeat_at_ms : Int64?
  mut last_reaper_at_ms : Int64?
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
    last_heartbeat_at_ms: None,
    last_reaper_at_ms: None,
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
pub async fn[B : @backend.QueueBackend] ConsumerService::start(
  self : ConsumerService[B],
) -> Unit {
  match self.lifecycle {
    Idle | Stopped => {
      self.lifecycle = Starting
      self.started_at_ms = Some((self.clock.now_ms)())
      self.stopped_at_ms = None
      self.last_heartbeat_at_ms = None
      self.last_reaper_at_ms = None
      self.lifecycle = Running
      @async.with_task_group(async fn(group) {
        let loop_task = group.spawn(
          async fn() {
            while self.lifecycle == Running ||
                  self.lifecycle == Starting ||
                  (self.lifecycle == Stopping && self.inflight_count > 0) {
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
              self.sleep_until_next_poll()
            }
          },
          no_wait=true,
        )
        ignore(loop_task.wait())
      })
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
  ignore(timeout_ms)
  match self.lifecycle {
    Running | Starting => self.lifecycle = Stopping
    Idle => {
      self.stopped_at_ms = Some((self.clock.now_ms)())
      self.lifecycle = Stopped
    }
    Stopping | Stopped => ()
  }
}
