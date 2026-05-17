///|
fn build_consumer_job(
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
fn[B : @backend.QueueBackend] enqueue_job(
  backend : B,
  job : @core.JobRecord,
) -> @core.JobId raise {
  backend.enqueue(job)
}

///|
fn[B : @backend.QueueBackend] stats_for_queue(
  backend : B,
  queue_name : String,
) -> @core.StatsSnapshot raise {
  backend.stats(queue_name)
}

///|
async test "mem backend drives consumer end-to-end" {
  let backend = new_mem_backend()
  let registry = @registry.new_registry()
  let handled_payloads : Array[Bytes] = []
  ignore(
    try! @registry.register_task(registry, {
      name: "mem.consumer",
      codec: @core.bytes_codec(),
      retry: @core.NoRetry,
      handler: fn(_context : @core.TaskContext, payload : Bytes) {
        handled_payloads.push(payload)
      },
      fallback: None,
    }),
  )
  let job_id = try! enqueue_job(
    backend,
    build_consumer_job("mem-job-1", "emails", "mem.consumer"),
  )
  @test.assert_eq(job_id, "mem-job-1")
  let consumer = @consumer.ConsumerService::new(backend, registry, {
    ..@consumer.default_consumer_options("emails", "consumer-a"),
    max_concurrency: 1,
    reserve_batch_size: 1,
    lease_ms: 100,
    poll_interval_ms: 10,
    pause_poll_interval_ms: 10,
  })

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    @async.sleep(60)
    consumer.stop(1000)
    ignore(start_task.wait())
  })

  @test.assert_eq(handled_payloads.length(), 1)
  @test.assert_eq(handled_payloads[0], b"payload")
  let stats = try! stats_for_queue(backend, "emails")
  @test.assert_eq(stats.succeeded, 1)
  @test.assert_eq(stats.pending, 0)
  @test.assert_eq(stats.dead, 0)
}
