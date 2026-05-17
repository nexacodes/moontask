///|
fn build_admin_job(
  job_id : String,
  queue_name : String,
  task_name : String,
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
    timeout_ms: None,
    deadline_ms: None,
    last_error: None,
    trace_id: None,
  }
}

///|
async fn enqueue_admin_job(
  backend : @memory.MemBackend,
  job : @core.JobRecord,
) -> @core.JobId {
  backend.enqueue(job)
}

///|
async fn wait_until_admin_stats(
  backend : @memory.MemBackend,
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
  fail("timed out waiting for admin stats")
}

///|
async test "admin client pauses and resumes queues while exposing queue stats" {
  let backend = @memory.new_mem_backend()
  let admin = AdminClient::new(backend)
  ignore(
    try! enqueue_admin_job(
      backend,
      build_admin_job("admin-job-1", "emails", "email.send"),
    ),
  )

  let before_pause = try! admin.queue_snapshot("emails")
  @test.assert_eq(before_pause.stats.pending, 1)
  @test.assert_eq(before_pause.stats.paused, false)

  try! admin.pause_queue("emails", "maintenance")
  let paused = try! admin.queue_snapshot("emails")
  @test.assert_eq(paused.stats.paused, true)

  try! admin.resume_queue("emails")
  let resumed = try! admin.queue_snapshot("emails")
  @test.assert_eq(resumed.stats.paused, false)
}

///|
async test "admin client lists jobs, loads jobs, retries dead jobs, cancels jobs, drains queues, and reports health" {
  let backend = @memory.new_mem_backend()
  let admin = AdminClient::new(backend)
  ignore(
    try! enqueue_admin_job(
      backend,
      build_admin_job("admin-job-3", "emails", "email.send"),
    ),
  )
  ignore(
    try! enqueue_admin_job(backend, {
      ..build_admin_job("admin-job-4", "emails", "email.send"),
      visible_at_ms: 50L,
    }),
  )
  ignore(
    try! enqueue_admin_job(
      backend,
      build_admin_job("admin-job-5", "invoices", "invoice.send"),
    ),
  )

  let first_page = try! admin.list_jobs("emails", 1)
  @test.assert_eq(first_page.jobs.length(), 1)
  @test.assert_eq(first_page.jobs[0].job_id, "admin-job-3")
  assert_true(first_page.next_cursor is Some("admin-job-3"))
  let next_cursor = match first_page.next_cursor {
    Some(cursor) => cursor
    None => fail("expected next cursor")
  }

  let second_page = try! admin.list_jobs("emails", 10, cursor=next_cursor)
  @test.assert_eq(second_page.jobs.length(), 1)
  @test.assert_eq(second_page.jobs[0].job_id, "admin-job-4")

  let loaded_job = try! admin.get_job("admin-job-4")
  assert_true(loaded_job is Some(_))
  @test.assert_eq(loaded_job.unwrap().queue_name, "emails")

  let scheduled_only = try! admin.list_jobs(
    "emails",
    10,
    status=@core.Scheduled,
  )
  @test.assert_eq(scheduled_only.jobs.length(), 1)
  @test.assert_eq(scheduled_only.jobs[0].job_id, "admin-job-4")

  let lease = backend.reserve_batch("emails", "consumer-a", 1, 100, 0L)[0]
  try! backend.nack_dead(lease.lease_id, "boom", 5L)
  @test.assert_eq(try! admin.retry_dead_job("admin-job-3"), true)
  @test.assert_eq(
    (try! admin.get_job("admin-job-3")).unwrap().status,
    @core.Pending,
  )

  @test.assert_eq(try! admin.cancel_job("admin-job-4"), true)
  @test.assert_eq(
    (try! admin.get_job("admin-job-4")).unwrap().status,
    @core.Cancelled,
  )

  @test.assert_eq(try! admin.drain_queue("invoices"), 1)
  @test.assert_eq(
    (try! admin.get_job("admin-job-5")).unwrap().status,
    @core.Cancelled,
  )

  let health = try! admin.health()
  @test.assert_eq(health.ok, true)
  @test.assert_eq(health.backend_kind, "memory")
  assert_true(health.detail is Some(_))
}

///|
async test "admin inspect and metrics expose live consumer state" {
  let backend = @memory.new_mem_backend()
  let registry = @registry.new_registry()
  let admin = AdminClient::new(backend)
  ignore(
    try! @registry.register_task(registry, {
      name: "admin.inspect",
      codec: @core.bytes_codec(),
      retry: @core.NoRetry,
      handler: async fn(_context : @core.TaskContext, _payload : Bytes) {
        @async.sleep(40)
      },
      fallback: None,
    }),
  )
  ignore(
    try! enqueue_admin_job(
      backend,
      build_admin_job("admin-job-2", "emails", "admin.inspect"),
    ),
  )
  let consumer = @consumer.ConsumerService::new(backend, registry, {
    ..@consumer.default_consumer_options("emails", "admin-consumer"),
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
      try! wait_until_admin_stats(backend, "emails", fn(stats) {
        stats.running == 1
      }),
    )

    let inspection = admin.inspect_consumer(consumer)
    @test.assert_eq(inspection.snapshot.lifecycle, @consumer.Running)
    @test.assert_eq(inspection.accepting_work, true)
    @test.assert_eq(inspection.inflight_leases.length(), 1)
    @test.assert_eq(inspection.inflight_leases[0].job.job_id, "admin-job-2")

    let running_metrics = try! admin.metrics(consumer)
    @test.assert_eq(running_metrics.running, 1)
    @test.assert_eq(running_metrics.succeeded, 0)
    @test.assert_eq(running_metrics.reserve_calls, 1)
    @test.assert_eq(running_metrics.processed_attempts, 0)

    consumer.stop(1000)
    let stopping_inspection = admin.inspect_consumer(consumer)
    @test.assert_eq(stopping_inspection.snapshot.lifecycle, @consumer.Stopping)
    @test.assert_eq(stopping_inspection.accepting_work, false)
    assert_true(stopping_inspection.stop_requested_at_ms is Some(_))
    assert_true(stopping_inspection.stop_deadline_ms is Some(_))

    ignore(start_task.wait())
  })

  let final_metrics = try! admin.metrics(consumer)
  @test.assert_eq(final_metrics.lifecycle, @consumer.Stopped)
  @test.assert_eq(final_metrics.succeeded, 1)
  @test.assert_eq(final_metrics.processed_attempts, 1)
  @test.assert_eq(final_metrics.succeeded_attempts, 1)
  @test.assert_eq(final_metrics.ack_calls, 1)
}
