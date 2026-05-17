///|
test "mem queue runtime plan advertises command-driven transitions" {
  let queue = @mem.MemQueue::new(
    @taskq.new_registry(),
    @taskq.default_queue_options("phase-two-plan"),
  )
  let plan = queue.runtime_plan()
  assert_true(plan.lifecycle is @runtime.QueueLifecycle::Running)
  assert_eq(plan.command_names.length(), 8)
  assert_eq(plan.command_names[0], "Enqueue")
  assert_eq(plan.command_names[1], "Dispatch")
  assert_eq(plan.command_names[2], "Ack")
  assert_eq(plan.command_names[3], "Nack")
  assert_eq(plan.command_names[4], "PollStats")
  assert_eq(plan.command_names[5], "Pause")
  assert_eq(plan.command_names[6], "Resume")
  assert_eq(plan.command_names[7], "Shutdown")
}

///|
async test "mem queue drains ready jobs without losing them" {
  let handled = []
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.process-all",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, payload : Bytes) {
      handled.push(payload)
    },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-two-mem"),
  )
  let first_job_id = try! queue.enqueue(
    task,
    b"first",
    @taskq.default_enqueue_options(),
  )
  let second_job_id = try! queue.enqueue(
    task,
    b"second",
    @taskq.default_enqueue_options(),
  )
  assert_eq(first_job_id, "phase-two-mem-job-1")
  assert_eq(second_job_id, "phase-two-mem-job-2")
  assert_eq(queue.job_count(), 2)
  assert_eq(queue.ready_job_count(), 2)

  let processed_count = queue.process_all()
  assert_eq(processed_count, 2)
  assert_eq(handled.length(), 2)
  assert_eq(handled[0], b"first")
  assert_eq(handled[1], b"second")
  assert_eq(queue.job_count(), 0)
  assert_eq(queue.ready_job_count(), 0)
  assert_eq(queue.inflight_job_count(), 0)

  let snapshot = queue.snapshot()
  assert_eq(snapshot.processed, 2)
  assert_eq(snapshot.succeeded, 2)
  assert_eq(snapshot.failed, 0)
  assert_eq(snapshot.queued, 0)
}

///|
async test "mem queue processes ready jobs concurrently" {
  let marks = []
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.concurrent",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: async fn(_context : @taskq.TaskContext, _payload : Bytes) {
      marks.push("start")
      @async.sleep(20)
      marks.push("end")
    },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(registry, {
    queue_name: "phase-two-concurrency",
    max_concurrency: 2,
    pause_after_failures: 0,
    rate_limit_per_second: None,
  })
  ignore(try! queue.enqueue(task, b"a", @taskq.default_enqueue_options()))
  ignore(try! queue.enqueue(task, b"b", @taskq.default_enqueue_options()))
  ignore(try! queue.enqueue(task, b"c", @taskq.default_enqueue_options()))

  let processed_count = queue.process_all()
  assert_eq(processed_count, 3)
  assert_eq(marks.length(), 6)
  assert_eq(marks[0], "start")
  assert_eq(marks[1], "start")
  assert_eq(marks[2], "end")
  assert_eq(queue.snapshot().succeeded, 3)
}

///|
async test "graceful shutdown waits for inflight worker completion" {
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.shutdown",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-two-shutdown"),
  )
  let job_id = try! queue.enqueue(
    task,
    b"payload",
    @taskq.default_enqueue_options(),
  )
  let lease = match queue.dispatch_next() {
    Some(lease) => lease
    None => fail("expected ready job to dispatch")
  }
  assert_eq(lease.job.id, job_id)
  assert_eq(queue.ready_job_count(), 0)
  assert_eq(queue.inflight_job_count(), 1)

  queue.shutdown_gracefully()
  assert_true(queue.lifecycle() is @runtime.QueueLifecycle::Closing)
  assert_eq(queue.inflight_job_count(), 1)

  let enqueue_after_shutdown : Result[@taskq.JobId, Error] = try? queue.enqueue(
    task,
    b"payload-2",
    @taskq.default_enqueue_options(),
  )
  guard enqueue_after_shutdown
    is Err(@taskq.QueueError::QueueClosed(queue_name)) &&
    queue_name == "phase-two-shutdown" else {
    fail("expected queue to reject new jobs while closing")
  }

  queue.complete_success(lease)
  assert_true(queue.lifecycle() is @runtime.QueueLifecycle::Closed)
  assert_eq(queue.inflight_job_count(), 0)
  assert_eq(queue.job_count(), 0)

  let snapshot = queue.snapshot()
  assert_eq(snapshot.processed, 1)
  assert_eq(snapshot.succeeded, 1)
}

///|
async test "command-driven manager flow updates failed job state" {
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.manager-flow",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-two-manager-flow"),
  )
  let job_id = try! queue.enqueue(
    task,
    b"payload",
    @taskq.default_enqueue_options(),
  )

  let lease = match queue.dispatch_next() {
    Some(lease) => lease
    None => fail("expected manager to dispatch a ready job")
  }
  assert_eq(lease.job.id, job_id)
  assert_eq(queue.ready_job_count(), 0)
  assert_eq(queue.inflight_job_count(), 1)

  queue.shutdown_gracefully()
  assert_true(queue.lifecycle() is @runtime.QueueLifecycle::Closing)

  queue.complete_failure(lease, "boom")
  assert_true(queue.lifecycle() is @runtime.QueueLifecycle::Closed)
  assert_eq(queue.job_count(), 0)
  assert_eq(queue.inflight_job_count(), 0)

  let snapshot = queue.snapshot()
  assert_eq(snapshot.queued, 0)
  assert_eq(snapshot.inflight, 0)
  assert_eq(snapshot.processed, 1)
  assert_eq(snapshot.succeeded, 0)
  assert_eq(snapshot.failed, 1)
  assert_eq(snapshot.dead, 1)
}

///|
async test "mem queue waits for delayed jobs within a reasonable range" {
  let started_at = []
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.delay",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) {
      started_at.push(@async.now())
    },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-three-delay"),
  )
  let enqueue_started_at = @async.now()
  ignore(
    try! queue.enqueue(task, b"later", {
      ..@taskq.default_enqueue_options(),
      delay_ms: 60,
    }),
  )
  assert_eq(queue.snapshot().queued, 0)
  assert_eq(queue.snapshot().delayed, 1)

  let processed_count = queue.process_all()
  let elapsed_ms = (@async.now() - enqueue_started_at).to_int()
  assert_eq(processed_count, 1)
  assert_eq(started_at.length(), 1)
  assert_true(elapsed_ms >= 40)
  assert_true(elapsed_ms < 300)
  assert_eq(queue.snapshot().succeeded, 1)
  assert_eq(queue.snapshot().delayed, 0)
}

///|
async test "mem queue retries with fixed delay" {
  let started_at = []
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.fixed-retry",
    codec: @taskq.bytes_codec(),
    retry: @taskq.Fixed(max_attempts=3, delay_ms=30),
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) raise {
      started_at.push(@async.now())
      if started_at.length() < 3 {
        raise @taskq.QueueError::StateInvariant("retry again")
      }
    },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-three-fixed-retry"),
  )
  ignore(try! queue.enqueue(task, b"retry", @taskq.default_enqueue_options()))

  let processed_count = queue.process_all()
  assert_eq(processed_count, 3)
  assert_eq(started_at.length(), 3)
  assert_true((started_at[1] - started_at[0]).to_int() >= 20)
  assert_true((started_at[2] - started_at[1]).to_int() >= 20)

  let snapshot = queue.snapshot()
  assert_eq(snapshot.processed, 3)
  assert_eq(snapshot.retried, 2)
  assert_eq(snapshot.succeeded, 1)
  assert_eq(snapshot.failed, 0)
  assert_eq(snapshot.dead, 0)
}

///|
async test "mem queue retries with exponential backoff" {
  let started_at = []
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.exponential-retry",
    codec: @taskq.bytes_codec(),
    retry: @taskq.Exponential(
      max_attempts=3,
      initial_delay_ms=20,
      factor=2.0,
      max_delay_ms=80,
    ),
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) raise {
      started_at.push(@async.now())
      if started_at.length() < 3 {
        raise @taskq.QueueError::StateInvariant("retry again")
      }
    },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-three-exponential-retry"),
  )
  ignore(try! queue.enqueue(task, b"retry", @taskq.default_enqueue_options()))

  let processed_count = queue.process_all()
  assert_eq(processed_count, 3)
  assert_eq(started_at.length(), 3)
  assert_true((started_at[1] - started_at[0]).to_int() >= 10)
  assert_true((started_at[2] - started_at[1]).to_int() >= 30)
  assert_eq(queue.snapshot().retried, 2)
  assert_eq(queue.snapshot().succeeded, 1)
}

///|
async test "mem queue sends exhausted retries to fallback and dead state" {
  let fallback_payloads = []
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.retry-dead",
    codec: @taskq.bytes_codec(),
    retry: @taskq.Fixed(max_attempts=2, delay_ms=20),
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) raise {
      raise @taskq.QueueError::StateInvariant("still failing")
    },
    fallback: Some(fn(
      _context : @taskq.TaskContext,
      payload : Bytes,
      _err : Error,
    ) {
      fallback_payloads.push(payload)
    }),
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-three-dead"),
  )
  ignore(try! queue.enqueue(task, b"payload", @taskq.default_enqueue_options()))

  let processed_count = queue.process_all()
  assert_eq(processed_count, 2)
  assert_eq(fallback_payloads.length(), 1)
  assert_eq(fallback_payloads[0], b"payload")

  let snapshot = queue.snapshot()
  assert_eq(snapshot.processed, 2)
  assert_eq(snapshot.retried, 1)
  assert_eq(snapshot.failed, 1)
  assert_eq(snapshot.dead, 1)
  assert_eq(queue.job_count(), 0)
}

///|
async test "mem queue enforces timeout and triggers fallback" {
  let fallback_calls = []
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.timeout",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: async fn(_context : @taskq.TaskContext, _payload : Bytes) {
      @async.sleep(120)
    },
    fallback: Some(fn(
      _context : @taskq.TaskContext,
      _payload : Bytes,
      _err : Error,
    ) {
      fallback_calls.push("fallback")
    }),
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-three-timeout"),
  )
  let started_at = @async.now()
  ignore(
    try! queue.enqueue(task, b"payload", {
      ..@taskq.default_enqueue_options(),
      timeout_ms: Some(40),
    }),
  )

  let processed_count = queue.process_all()
  let elapsed_ms = (@async.now() - started_at).to_int()
  assert_eq(processed_count, 1)
  assert_eq(fallback_calls.length(), 1)
  assert_true(elapsed_ms < 120)

  let snapshot = queue.snapshot()
  assert_eq(snapshot.processed, 1)
  assert_eq(snapshot.failed, 1)
  assert_eq(snapshot.dead, 1)
}

///|
async test "mem queue rejects duplicate jobs by dedupe key and releases after completion" {
  let handled = []
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.dedupe",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, payload : Bytes) {
      handled.push(payload)
    },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-four-dedupe"),
  )
  ignore(
    try! queue.enqueue(task, b"first", {
      ..@taskq.default_enqueue_options(),
      dedupe_key: Some("same-key"),
    }),
  )

  let duplicate_result : Result[@taskq.JobId, Error] = try? queue.enqueue(
    task,
    b"second",
    { ..@taskq.default_enqueue_options(), dedupe_key: Some("same-key") },
  )
  guard duplicate_result is Err(@taskq.QueueError::DuplicateJob(dedupe_key)) &&
    dedupe_key == "same-key" else {
    fail("expected duplicate dedupe key to be rejected")
  }

  assert_eq(queue.process_all(), 1)
  assert_eq(handled.length(), 1)
  assert_eq(handled[0], b"first")

  let next_job_id = try! queue.enqueue(task, b"third", {
    ..@taskq.default_enqueue_options(),
    dedupe_key: Some("same-key"),
  })
  assert_eq(next_job_id, "phase-four-dedupe-job-2")
}

///|
async test "mem queue pauses after continuous failures and resumes processing" {
  let handled = []
  let registry = @taskq.new_registry()
  let failing_task = try! @taskq.register_task(registry, {
    name: "mem.pause.fail",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) raise {
      raise @taskq.QueueError::StateInvariant("boom")
    },
    fallback: None,
  })
  let success_task = try! @taskq.register_task(registry, {
    name: "mem.pause.success",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, payload : Bytes) {
      handled.push(payload)
    },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(registry, {
    queue_name: "phase-four-pause",
    max_concurrency: 1,
    pause_after_failures: 1,
    rate_limit_per_second: None,
  })

  ignore(
    try! queue.enqueue(failing_task, b"fail", @taskq.default_enqueue_options()),
  )
  assert_eq(queue.process_all(), 1)
  assert_true(queue.snapshot().paused)
  assert_eq(queue.snapshot().failed, 1)

  ignore(
    try! queue.enqueue(success_task, b"ok", @taskq.default_enqueue_options()),
  )
  assert_eq(queue.process_all(), 0)
  assert_eq(handled.length(), 0)

  queue.resume_consuming()
  assert_false(queue.snapshot().paused)
  assert_eq(queue.process_all(), 1)
  assert_eq(handled.length(), 1)
  assert_eq(handled[0], b"ok")
}

///|
async test "mem queue honors local rate limit" {
  let started_at = []
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.rate-limit",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) {
      started_at.push(@async.now())
    },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(registry, {
    queue_name: "phase-four-rate-limit",
    max_concurrency: 1,
    pause_after_failures: 0,
    rate_limit_per_second: Some(2),
  })
  ignore(try! queue.enqueue(task, b"a", @taskq.default_enqueue_options()))
  ignore(try! queue.enqueue(task, b"b", @taskq.default_enqueue_options()))
  ignore(try! queue.enqueue(task, b"c", @taskq.default_enqueue_options()))

  assert_eq(queue.process_all(), 3)
  assert_eq(started_at.length(), 3)
  assert_true((started_at[2] - started_at[1]).to_int() >= 350)
}
