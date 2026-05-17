///|
async fn[B : @backend.QueueBackend] reserve_one_visible(
  backend : B,
  queue_name : String,
  now_ms : Int64,
) -> @core.Lease {
  let leases = backend.reserve_batch(
    queue_name, "producer-test", 1, 1000, now_ms,
  )
  guard leases.length() == 1 else { fail("expected one visible lease") }
  leases[0]
}

///|
async test "producer enqueues typed payloads with task retry defaults" {
  let clock = @testing.new_fake_clock(1000L)
  let backend = @memory.new_mem_backend()
  let producer = Producer::new_with_runtime(backend, "emails", {
    clock: clock.runtime_clock(),
  })
  let registry = @registry.new_registry()
  let task = try! @registry.register_task(registry, {
    name: "email.send",
    codec: @core.bytes_codec(),
    retry: @core.Fixed(max_attempts=5, delay_ms=200),
    handler: fn(_context : @core.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })

  let job_id = try! producer.enqueue(task, b"hello")
  @test.assert_eq(job_id, "job-1000-1")

  let lease = try! reserve_one_visible(backend, "emails", 1000L)
  @test.assert_eq(lease.job.job_id, job_id)
  @test.assert_eq(lease.job.task_name, "email.send")
  @test.assert_eq(lease.job.payload, b"hello")
  @test.assert_eq(lease.job.queue_name, "emails")
  @test.assert_eq(lease.job.max_attempts, 5)
  @test.assert_eq(lease.job.created_at_ms, 1000L)
  @test.assert_eq(lease.job.visible_at_ms, 1000L)
  @test.assert_eq(lease.job.timeout_ms, None)
  @test.assert_eq(lease.job.dedupe_key, None)
}

///|
async test "producer applies enqueue options to scheduled jobs" {
  let clock = @testing.new_fake_clock(1000L)
  let backend = @memory.new_mem_backend()
  let producer = Producer::new_with_runtime(backend, "emails", {
    clock: clock.runtime_clock(),
  })
  let registry = @registry.new_registry()
  let task = try! @registry.register_task(registry, {
    name: "email.send",
    codec: @core.bytes_codec(),
    retry: @core.NoRetry,
    handler: fn(_context : @core.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })

  let job_id = try! producer.enqueue_with_options(task, b"payload", {
    delay_ms: 50,
    dedupe_key: Some("dedupe-email-1"),
    schedule_at_ms: Some(2000L),
    timeout_ms: Some(5000),
    max_attempts_override: Some(9),
  })
  @test.assert_eq(job_id, "job-1000-1")
  @test.assert_eq(
    try! backend
    .reserve_batch("emails", "producer-test", 1, 1000, 1500L)
    .length(),
    0,
  )

  let lease = try! reserve_one_visible(backend, "emails", 2000L)
  @test.assert_eq(lease.job.visible_at_ms, 2000L)
  @test.assert_eq(lease.job.max_attempts, 9)
  @test.assert_eq(lease.job.timeout_ms, Some(5000))
  @test.assert_eq(lease.job.dedupe_key, Some("dedupe-email-1"))
}

///|
async test "producer generates monotonic job ids across multiple enqueues" {
  let clock = @testing.new_fake_clock(42L)
  let backend = @memory.new_mem_backend()
  let producer = Producer::new_with_runtime(backend, "emails", {
    clock: clock.runtime_clock(),
  })
  let registry = @registry.new_registry()
  let task = try! @registry.register_task(registry, {
    name: "email.send",
    codec: @core.bytes_codec(),
    retry: @core.NoRetry,
    handler: fn(_context : @core.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })

  let first = try! producer.enqueue(task, b"a")
  let second = try! producer.enqueue(task, b"b")

  @test.assert_eq(first, "job-42-1")
  @test.assert_eq(second, "job-42-2")
}
