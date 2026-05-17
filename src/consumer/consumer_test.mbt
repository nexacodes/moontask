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
fn[B : @backend.QueueBackend] enqueue_job(
  backend : B,
  job : @core.JobRecord,
) -> @core.JobId raise {
  backend.enqueue(job)
}

///|
async fn[B : @backend.QueueBackend] wait_until_stats(
  backend : B,
  queue_name : String,
  done : (@core.StatsSnapshot) -> Bool,
) -> @core.StatsSnapshot {
  for _ in 0..<100 {
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
}
