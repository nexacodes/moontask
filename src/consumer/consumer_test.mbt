///|
fn build_consumer_job(
  job_id : String,
  queue_name : String,
  task_name : String,
  timeout_ms? : Int,
) -> @core.JobRecord {
  {
    job_id,
    queue_name,
    task_name,
    payload: b"payload",
    status: @core.Pending,
    attempt: 1,
    max_attempts: 3,
    dedupe_key: None,
    created_at_ms: 0L,
    visible_at_ms: 0L,
    lease_id: None,
    lease_owner: None,
    lease_deadline_ms: None,
    started_at_ms: None,
    finished_at_ms: None,
    timeout_ms,
    deadline_ms: None,
    last_error: None,
    trace_id: None,
  }
}

///|
async fn[B : @backend.QueueBackend] enqueue_job(
  backend : B,
  job : @core.JobRecord,
) -> @core.JobId {
  backend.enqueue(job)
}

///|
async fn[B : @backend.QueueBackend] wait_until_stats(
  backend : B,
  queue_name : String,
  done : (@core.StatsSnapshot) -> Bool,
) -> @core.StatsSnapshot {
  for _ in 0..<300 {
    let stats = backend.stats(queue_name)
    if done(stats) {
      return stats
    }
    @async.sleep(10)
  }
  raise @core.QueueError::StateInvariant("timed out waiting for queue stats")
}

///|
struct RecordingHook {
  before_job_ids : Array[String]
  after_outcomes : Array[@core.AttemptOutcome]
}

///|
fn new_recording_hook() -> RecordingHook {
  { before_job_ids: [], after_outcomes: [] }
}

///|
pub impl @core.Hook for RecordingHook with before_process(self, event) {
  self.before_job_ids.push(event.job_id)
}

///|
pub impl @core.Hook for RecordingHook with after_process(self, _event, result) {
  self.after_outcomes.push(result.outcome)
}

///|
struct FailingHook {
  mut before_calls : Int
  mut after_calls : Int
}

///|
fn new_failing_hook() -> FailingHook {
  { before_calls: 0, after_calls: 0 }
}

///|
pub impl @core.Hook for FailingHook with before_process(self, _event) raise {
  self.before_calls = self.before_calls + 1
  raise @core.QueueError::BackendFailure("before hook failed")
}

///|
pub impl @core.Hook for FailingHook with after_process(self, _event, _result) raise {
  self.after_calls = self.after_calls + 1
  raise @core.QueueError::BackendFailure("after hook failed")
}

///|
async test "consumer service start and stop update lifecycle snapshot" {
  let backend = @memory.new_mem_backend()
  let registry = @registry.new_registry()
  let consumer = ConsumerService::new(
    backend,
    registry,
    default_consumer_options("emails", "consumer-a"),
  )

  assert_true(consumer.lifecycle() is Idle)
  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    @async.sleep(20)
    assert_true(consumer.lifecycle() is Running)
    @test.assert_eq(consumer.snapshot().queue_name, "emails")
    @test.assert_eq(consumer.snapshot().consumer_id, "consumer-a")
    consumer.stop(1000)
    ignore(start_task.wait())
  })
  assert_true(consumer.lifecycle() is Stopped)
}

///|
async test "consumer graceful stop waits for in-flight work before stopping" {
  let backend = @memory.new_mem_backend()
  ignore(
    try! enqueue_job(
      backend,
      build_consumer_job("graceful-job-1", "emails", "consumer.graceful"),
    ),
  )
  let registry = @registry.new_registry()
  let completed_job_ids : Array[String] = []
  ignore(
    try! @registry.register_task(registry, {
      name: "consumer.graceful",
      codec: @core.bytes_codec(),
      retry: @core.NoRetry,
      handler: async fn(context : @core.TaskContext, _payload : Bytes) {
        @async.sleep(30)
        completed_job_ids.push(context.job_id)
      },
      fallback: None,
    }),
  )
  let consumer = ConsumerService::new(backend, registry, {
    ..default_consumer_options("emails", "consumer-a"),
    poll_interval_ms: 5,
    pause_poll_interval_ms: 5,
    heartbeat_interval_ms: 5,
    reaper_interval_ms: 5,
  })

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    ignore(
      try! wait_until_stats(backend, "emails", fn(stats) { stats.running == 1 }),
    )
    consumer.stop(1000)
    let stopping_inspection = consumer.inspect()
    @test.assert_eq(stopping_inspection.snapshot.lifecycle, Stopping)
    @test.assert_eq(stopping_inspection.accepting_work, false)
    assert_true(stopping_inspection.stop_requested_at_ms is Some(_))
    assert_true(stopping_inspection.stop_deadline_ms is Some(_))
    ignore(start_task.wait())
  })

  let final_stats = try! backend.stats("emails")
  @test.assert_eq(final_stats.succeeded, 1)
  @test.assert_eq(completed_job_ids, ["graceful-job-1"])
  @test.assert_eq(consumer.inspect().inflight_leases.length(), 0)
  assert_true(consumer.lifecycle() is Stopped)
}

///|
async test "consumer runtime honors max concurrency during resident processing" {
  let backend = @memory.new_mem_backend()
  ignore(
    try! enqueue_job(
      backend,
      build_consumer_job("job-1", "emails", "consumer.test"),
    ),
  )
  ignore(
    try! enqueue_job(
      backend,
      build_consumer_job("job-2", "emails", "consumer.test"),
    ),
  )
  let registry = @registry.new_registry()
  let handled_job_ids : Array[String] = []
  ignore(
    try! @registry.register_task(registry, {
      name: "consumer.test",
      codec: @core.bytes_codec(),
      retry: @core.NoRetry,
      handler: async fn(context : @core.TaskContext, _payload : Bytes) {
        handled_job_ids.push(context.job_id)
        @async.sleep(50)
      },
      fallback: None,
    }),
  )
  let consumer = ConsumerService::new(backend, registry, {
    ..default_consumer_options("emails", "consumer-a"),
    max_concurrency: 1,
    reserve_batch_size: 2,
    poll_interval_ms: 10,
    pause_poll_interval_ms: 10,
  })

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    @async.sleep(20)
    @test.assert_eq(consumer.snapshot().inflight_count, 1)
    @test.assert_eq(consumer.snapshot().available_worker_slots, 0)
    consumer.stop(1000)
    ignore(start_task.wait())
  })
  @test.assert_eq(handled_job_ids.length(), 1)
  @test.assert_eq(handled_job_ids[0], "job-1")
}

///|
async test "consumer start loop automatically reserves and acknowledges jobs" {
  let backend = @memory.new_mem_backend()
  ignore(
    try! enqueue_job(
      backend,
      build_consumer_job("job-1", "emails", "consumer.test"),
    ),
  )
  let registry = @registry.new_registry()
  let handled_job_ids : Array[String] = []
  ignore(
    try! @registry.register_task(registry, {
      name: "consumer.test",
      codec: @core.bytes_codec(),
      retry: @core.NoRetry,
      handler: fn(context : @core.TaskContext, _payload : Bytes) {
        handled_job_ids.push(context.job_id)
      },
      fallback: None,
    }),
  )
  let consumer = ConsumerService::new(backend, registry, {
    ..default_consumer_options("emails", "consumer-a"),
    poll_interval_ms: 10,
    pause_poll_interval_ms: 10,
  })

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    @async.sleep(40)
    consumer.stop(1000)
    ignore(start_task.wait())
  })

  let stats = try! backend.stats("emails")
  @test.assert_eq(stats.succeeded, 1)
  @test.assert_eq(handled_job_ids, ["job-1"])
  assert_true(consumer.lifecycle() is Stopped)
}

///|
async test "consumer stop timeout cancels in-flight work and allows later redelivery" {
  let backend = @memory.new_mem_backend()
  ignore(
    try! enqueue_job(
      backend,
      build_consumer_job("force-job-1", "emails", "consumer.force-stop"),
    ),
  )

  let first_registry = @registry.new_registry()
  let started_job_ids : Array[String] = []
  let cancelled_job_ids : Array[String] = []
  let completed_job_ids : Array[String] = []
  ignore(
    try! @registry.register_task(first_registry, {
      name: "consumer.force-stop",
      codec: @core.bytes_codec(),
      retry: @core.NoRetry,
      handler: async fn(context : @core.TaskContext, _payload : Bytes) {
        started_job_ids.push(context.job_id)
        @async.sleep(50) catch {
          err if @async.is_cancellation_error(err) => {
            cancelled_job_ids.push(context.job_id)
            raise err
          }
          err => raise err
        }
        completed_job_ids.push(context.job_id)
      },
      fallback: None,
    }),
  )
  let first_consumer = ConsumerService::new(backend, first_registry, {
    ..default_consumer_options("emails", "consumer-a"),
    lease_ms: 15,
    poll_interval_ms: 5,
    pause_poll_interval_ms: 5,
    heartbeat_interval_ms: 1000,
    reaper_interval_ms: 1000,
  })

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! first_consumer.start() },
      no_wait=true,
    )
    ignore(
      try! wait_until_stats(backend, "emails", fn(stats) { stats.running == 1 }),
    )
    let running_inspection = first_consumer.inspect()
    @test.assert_eq(running_inspection.inflight_leases.length(), 1)
    first_consumer.stop(5)
    ignore(start_task.wait())
  })

  @test.assert_eq(started_job_ids, ["force-job-1"])
  @test.assert_eq(cancelled_job_ids, ["force-job-1"])
  @test.assert_eq(completed_job_ids.length(), 0)
  let post_stop_stats = try! backend.stats("emails")
  @test.assert_eq(post_stop_stats.succeeded, 0)
  @test.assert_eq(post_stop_stats.dead, 0)
  assert_true(first_consumer.lifecycle() is Stopped)
}

///|
async test "consumer retries a failed job and eventually succeeds" {
  let backend = @memory.new_mem_backend()
  ignore(
    try! enqueue_job(
      backend,
      build_consumer_job("retry-job-1", "emails", "consumer.retry"),
    ),
  )
  let registry = @registry.new_registry()
  let attempts : Array[Int] = []
  ignore(
    try! @registry.register_task(registry, {
      name: "consumer.retry",
      codec: @core.bytes_codec(),
      retry: @core.Fixed(max_attempts=3, delay_ms=10),
      handler: fn(context : @core.TaskContext, _payload : Bytes) raise {
        attempts.push(context.attempt)
        if context.attempt == 1 {
          raise @core.QueueError::BackendFailure("retry me")
        }
      },
      fallback: None,
    }),
  )
  let consumer = ConsumerService::new(backend, registry, {
    ..default_consumer_options("emails", "consumer-a"),
    poll_interval_ms: 5,
    pause_poll_interval_ms: 5,
    heartbeat_interval_ms: 5,
    reaper_interval_ms: 5,
  })

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    let final_stats = try! wait_until_stats(backend, "emails", fn(stats) {
      stats.succeeded == 1
    })
    @test.assert_eq(final_stats.dead, 0)
    consumer.stop(1000)
    ignore(start_task.wait())
  })

  @test.assert_eq(attempts, [1, 2])
  let metrics = consumer.runtime_metrics()
  @test.assert_eq(metrics.processed_attempts, 2)
  @test.assert_eq(metrics.succeeded_attempts, 1)
  @test.assert_eq(metrics.failed_retry_attempts, 1)
  @test.assert_eq(metrics.failed_dead_attempts, 0)
  @test.assert_eq(metrics.ack_calls, 1)
  @test.assert_eq(metrics.nack_retry_calls, 1)
  @test.assert_eq(metrics.nack_dead_calls, 0)
  @test.assert_eq(consumer.inspect().pending_ack_count, 0)
}

///|
async test "consumer applies exponential retry delays before success" {
  let backend = @memory.new_mem_backend()
  ignore(
    try! enqueue_job(backend, {
      ..build_consumer_job("retry-exp-job-1", "emails", "consumer.retry.exp"),
      max_attempts: 4,
    }),
  )
  let registry = @registry.new_registry()
  let attempt_starts_ms : Array[Int64] = []
  ignore(
    try! @registry.register_task(registry, {
      name: "consumer.retry.exp",
      codec: @core.bytes_codec(),
      retry: @core.Exponential(
        max_attempts=4,
        initial_delay_ms=5,
        factor=2.0,
        max_delay_ms=20,
      ),
      handler: fn(context : @core.TaskContext, _payload : Bytes) raise {
        attempt_starts_ms.push(context.started_at_ms)
        if context.attempt < 4 {
          raise @core.QueueError::BackendFailure("retry with backoff")
        }
      },
      fallback: None,
    }),
  )
  let consumer = ConsumerService::new_with_runtime(
    backend,
    registry,
    {
      ..default_consumer_options("emails", "consumer-a"),
      poll_interval_ms: 1,
      pause_poll_interval_ms: 1,
      heartbeat_interval_ms: 1000,
      reaper_interval_ms: 1000,
    },
    default_consumer_runtime_options(),
  )

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    ignore(
      try! wait_until_stats(backend, "emails", fn(stats) {
        stats.succeeded == 1
      }),
    )
    consumer.stop(1000)
    ignore(start_task.wait())
  })

  @test.assert_eq(attempt_starts_ms.length(), 4)
  assert_true(attempt_starts_ms[1] - attempt_starts_ms[0] >= 5L)
  assert_true(attempt_starts_ms[2] - attempt_starts_ms[1] >= 10L)
  assert_true(attempt_starts_ms[3] - attempt_starts_ms[2] >= 20L)
}

///|
async test "consumer marks no-retry failures dead and ignores fallback errors" {
  let backend = @memory.new_mem_backend()
  ignore(
    try! enqueue_job(
      backend,
      build_consumer_job("dead-job-1", "emails", "consumer.dead"),
    ),
  )
  let registry = @registry.new_registry()
  let fallback_job_ids : Array[String] = []
  ignore(
    try! @registry.register_task(registry, {
      name: "consumer.dead",
      codec: @core.bytes_codec(),
      retry: @core.NoRetry,
      handler: fn(_context : @core.TaskContext, _payload : Bytes) raise {
        raise @core.QueueError::BackendFailure("fatal")
      },
      fallback: Some(fn(
        context : @core.TaskContext,
        _payload : Bytes,
        _err : Error,
      ) raise {
        fallback_job_ids.push(context.job_id)
        raise @core.QueueError::BackendFailure("fallback failed")
      }),
    }),
  )
  let consumer = ConsumerService::new(backend, registry, {
    ..default_consumer_options("emails", "consumer-a"),
    poll_interval_ms: 5,
    pause_poll_interval_ms: 5,
    heartbeat_interval_ms: 5,
    reaper_interval_ms: 5,
  })

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    let final_stats = try! wait_until_stats(backend, "emails", fn(stats) {
      stats.dead == 1
    })
    @test.assert_eq(final_stats.succeeded, 0)
    consumer.stop(1000)
    ignore(start_task.wait())
  })

  @test.assert_eq(fallback_job_ids, ["dead-job-1"])
  let metrics = consumer.runtime_metrics()
  @test.assert_eq(metrics.processed_attempts, 1)
  @test.assert_eq(metrics.succeeded_attempts, 0)
  @test.assert_eq(metrics.failed_retry_attempts, 0)
  @test.assert_eq(metrics.failed_dead_attempts, 1)
  @test.assert_eq(metrics.ack_calls, 0)
  @test.assert_eq(metrics.nack_retry_calls, 0)
  @test.assert_eq(metrics.nack_dead_calls, 1)
  @test.assert_eq(consumer.inspect().pending_ack_count, 0)
}

///|
async test "consumer ignores hook errors while still reporting success" {
  let backend = @memory.new_mem_backend()
  ignore(
    try! enqueue_job(
      backend,
      build_consumer_job("hook-job-1", "emails", "consumer.hooks"),
    ),
  )
  let registry = @registry.new_registry()
  let recording_hook = new_recording_hook()
  let failing_hook = new_failing_hook()
  ignore(
    try! @registry.register_task(registry, {
      name: "consumer.hooks",
      codec: @core.bytes_codec(),
      retry: @core.NoRetry,
      handler: fn(_context : @core.TaskContext, _payload : Bytes) { () },
      fallback: None,
    }),
  )
  let consumer = ConsumerService::new_with_runtime(
    backend,
    registry,
    {
      ..default_consumer_options("emails", "consumer-a"),
      poll_interval_ms: 5,
      pause_poll_interval_ms: 5,
      heartbeat_interval_ms: 5,
      reaper_interval_ms: 5,
    },
    {
      hooks: [recording_hook as &@core.Hook, failing_hook as &@core.Hook],
      clock: @core.system_runtime_clock(),
    },
  )

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    ignore(
      try! wait_until_stats(backend, "emails", fn(stats) {
        stats.succeeded == 1
      }),
    )
    consumer.stop(1000)
    ignore(start_task.wait())
  })

  @test.assert_eq(recording_hook.before_job_ids, ["hook-job-1"])
  @test.assert_eq(recording_hook.after_outcomes, [@core.Succeeded])
  @test.assert_eq(failing_hook.before_calls, 1)
  @test.assert_eq(failing_hook.after_calls, 1)
}

///|
async test "consumer keeps long running jobs leased, marks them running, and retries timed out attempts" {
  let backend = @memory.new_mem_backend()
  ignore(
    try! enqueue_job(
      backend,
      build_consumer_job(
        "timeout-job-1",
        "emails",
        "consumer.timeout",
        timeout_ms=10,
      ),
    ),
  )
  let registry = @registry.new_registry()
  let attempts : Array[Int] = []
  ignore(
    try! @registry.register_task(registry, {
      name: "consumer.timeout",
      codec: @core.bytes_codec(),
      retry: @core.Fixed(max_attempts=2, delay_ms=5),
      handler: async fn(context : @core.TaskContext, _payload : Bytes) {
        attempts.push(context.attempt)
        if context.attempt == 1 {
          @async.sleep(30)
        }
      },
      fallback: None,
    }),
  )
  let consumer = ConsumerService::new(backend, registry, {
    ..default_consumer_options("emails", "consumer-a"),
    lease_ms: 15,
    poll_interval_ms: 5,
    pause_poll_interval_ms: 5,
    heartbeat_interval_ms: 5,
    reaper_interval_ms: 5,
  })

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    let running_stats = try! wait_until_stats(backend, "emails", fn(stats) {
      stats.running == 1
    })
    @test.assert_eq(running_stats.reserved, 0)
    let final_stats = try! wait_until_stats(backend, "emails", fn(stats) {
      stats.succeeded == 1
    })
    @test.assert_eq(final_stats.dead, 0)
    consumer.stop(1000)
    ignore(start_task.wait())
  })

  @test.assert_eq(attempts, [1, 2])
}

///|
async test "consumer start loop runs heartbeat and reaper cycles for in-flight work" {
  let backend = @memory.new_mem_backend()
  ignore(
    try! enqueue_job(
      backend,
      build_consumer_job("job-heartbeat-1", "emails", "consumer.heartbeat"),
    ),
  )
  let registry = @registry.new_registry()
  let attempts : Array[Int] = []
  ignore(
    try! @registry.register_task(registry, {
      name: "consumer.heartbeat",
      codec: @core.bytes_codec(),
      retry: @core.NoRetry,
      handler: async fn(context : @core.TaskContext, _payload : Bytes) {
        attempts.push(context.attempt)
        @async.sleep(40)
      },
      fallback: None,
    }),
  )
  let consumer = ConsumerService::new(backend, registry, {
    ..default_consumer_options("emails", "consumer-a"),
    lease_ms: 15,
    poll_interval_ms: 5,
    pause_poll_interval_ms: 5,
    heartbeat_interval_ms: 5,
    reaper_interval_ms: 5,
  })

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    let running_stats = try! wait_until_stats(backend, "emails", fn(stats) {
      stats.running == 1
    })
    @test.assert_eq(running_stats.succeeded, 0)
    let final_stats = try! wait_until_stats(backend, "emails", fn(stats) {
      stats.succeeded == 1
    })
    @test.assert_eq(final_stats.dead, 0)
    consumer.stop(1000)
    ignore(start_task.wait())
  })

  @test.assert_eq(attempts, [1])
  let metrics = consumer.runtime_metrics()
  assert_true(metrics.lease_extensions > 0)
  @test.assert_eq(metrics.expired_leases_reaped, 0)
  @test.assert_eq(metrics.succeeded_attempts, 1)
}
