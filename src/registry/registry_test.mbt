///|
test "register_task assigns stable ids and stores task names" {
  let registry = new_registry()
  let task = try! register_task(registry, {
    name: "email.send",
    codec: @core.bytes_codec(),
    retry: @core.NoRetry,
    handler: fn(_context : @core.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })

  @test.assert_eq(task.id, 1)
  @test.assert_eq(task.name, "email.send")
  assert_true(registry.contains("email.send"))
  @test.assert_eq(registry.task_count(), 1)
  @test.assert_eq(registry.task_names(), ["email.send"])
}

///|
fn failing_bytes_codec() -> @core.Codec[Bytes] {
  {
    encode: fn(payload : Bytes) { payload },
    decode: fn(_payload : Bytes) raise {
      raise @core.QueueError::BackendFailure("decode failed")
    },
  }
}

///|
fn build_registry_job(task_name : String) -> @core.JobRecord {
  {
    job_id: "registry-job-1",
    queue_name: "emails",
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
fn build_registry_context() -> @core.TaskContext {
  {
    queue_name: "emails",
    job_id: "registry-job-1",
    attempt: 1,
    max_attempts: 3,
    enqueued_at_ms: 0L,
    started_at_ms: 1L,
    deadline_ms: None,
    dedupe_key: None,
  }
}

///|
test "register_task rejects duplicate task names" {
  let registry = new_registry()
  ignore(
    try! register_task(registry, {
      name: "email.send",
      codec: @core.bytes_codec(),
      retry: @core.NoRetry,
      handler: fn(_context : @core.TaskContext, _payload : Bytes) { () },
      fallback: None,
    }),
  )

  let duplicate_result : Result[@core.Task[Bytes], Error] = try? register_task(
    registry,
    {
      name: "email.send",
      codec: @core.bytes_codec(),
      retry: @core.NoRetry,
      handler: fn(_context : @core.TaskContext, _payload : Bytes) { () },
      fallback: None,
    },
  )
  guard duplicate_result is Err(@core.QueueError::DuplicateTask("email.send")) else {
    fail("expected duplicate task name to be rejected")
  }
}

///|
test "max_attempts_for_task respects explicit override" {
  let registry = new_registry()
  let task = try! register_task(registry, {
    name: "email.retry",
    codec: @core.bytes_codec(),
    retry: @core.Exponential(
      max_attempts=5,
      initial_delay_ms=10,
      factor=2.0,
      max_delay_ms=100,
    ),
    handler: fn(_context : @core.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })

  @test.assert_eq(max_attempts_for_task(task, None), 5)
  @test.assert_eq(max_attempts_for_task(task, Some(2)), 2)
}

///|
async test "registry reports missing tasks on run and policy lookup" {
  let registry = new_registry()
  let context = build_registry_context()
  let missing_job = build_registry_job("missing.task")

  let run_result : Result[Unit, Error] = try? run_registered_job(
    registry, context, missing_job,
  )
  guard run_result is Err(@core.QueueError::TaskNotFound("missing.task")) else {
    fail("expected missing task run to fail")
  }

  let fallback_result : Result[Unit, Error] = try? run_registered_fallback(
    registry,
    context,
    missing_job,
    @core.QueueError::BackendFailure("boom"),
  )
  guard fallback_result is Err(@core.QueueError::TaskNotFound("missing.task")) else {
    fail("expected missing task fallback to fail")
  }

  let policy_result : Result[@core.RetryPolicy, Error] = try? registry_retry_policy(
    registry, "missing.task",
  )
  guard policy_result is Err(@core.QueueError::TaskNotFound("missing.task")) else {
    fail("expected missing task policy lookup to fail")
  }
}

///|
async test "registry propagates decode failures for handler and fallback" {
  let registry = new_registry()
  ignore(
    try! register_task(registry, {
      name: "email.decode",
      codec: failing_bytes_codec(),
      retry: @core.NoRetry,
      handler: fn(_context : @core.TaskContext, _payload : Bytes) { () },
      fallback: Some(fn(
        _context : @core.TaskContext,
        _payload : Bytes,
        _err : Error,
      ) {
        ()
      }),
    }),
  )
  let context = build_registry_context()
  let job = build_registry_job("email.decode")

  let run_result : Result[Unit, Error] = try? run_registered_job(
    registry, context, job,
  )
  guard run_result is Err(@core.QueueError::BackendFailure("decode failed")) else {
    fail("expected decode failure to reach handler path")
  }

  let fallback_result : Result[Unit, Error] = try? run_registered_fallback(
    registry,
    context,
    job,
    @core.QueueError::BackendFailure("original"),
  )
  guard fallback_result
    is Err(@core.QueueError::BackendFailure("decode failed")) else {
    fail("expected decode failure to reach fallback path")
  }
}
