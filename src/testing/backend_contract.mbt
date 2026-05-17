///|
pub fn build_backend_contract_job(
  job_id : String,
  queue_name : String,
  visible_at_ms : Int64,
  dedupe_key? : String,
) -> @core.JobRecord {
  {
    job_id,
    queue_name,
    task_name: "backend.contract",
    payload: b"payload",
    status: if visible_at_ms > 0L {
      @core.Scheduled
    } else {
      @core.Pending
    },
    attempt: 1,
    max_attempts: 3,
    dedupe_key,
    created_at_ms: 0L,
    visible_at_ms,
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
pub async fn[B : @backend.QueueBackend] assert_backend_contract_reserve_and_ack(
  backend : B,
) -> Unit {
  let job_id = backend.enqueue(
    build_backend_contract_job("job-1", "emails", 0L),
  )
  @test.assert_eq(job_id, "job-1")

  let leases = backend.reserve_batch("emails", "consumer-a", 10, 5000, 100L)
  @test.assert_eq(leases.length(), 1)
  @test.assert_eq(leases[0].job.job_id, "job-1")
  assert_true(leases[0].job.status is @core.Reserved)

  backend.ack(leases[0].lease_id, 140L)
  let stats = backend.stats("emails")
  @test.assert_eq(stats.succeeded, 1)
  @test.assert_eq(stats.reserved, 0)
  @test.assert_eq(stats.pending, 0)

  let second_leases = backend.reserve_batch(
    "emails", "consumer-b", 1, 5000, 200L,
  )
  @test.assert_eq(second_leases.length(), 0)
}

///|
pub async fn[B : @backend.QueueBackend] assert_backend_contract_lease_exclusivity(
  backend : B,
) -> Unit {
  ignore(
    backend.enqueue(build_backend_contract_job("job-lease-1", "emails", 0L)),
  )

  let first = backend.reserve_batch("emails", "consumer-a", 1, 100, 100L)
  @test.assert_eq(first.length(), 1)

  let second = backend.reserve_batch("emails", "consumer-b", 1, 100, 101L)
  @test.assert_eq(second.length(), 0)
}

///|
pub async fn[B : @backend.QueueBackend] assert_backend_contract_expired_lease_redelivery(
  backend : B,
) -> Unit {
  ignore(backend.enqueue(build_backend_contract_job("job-2", "emails", 0L)))

  let leases = backend.reserve_batch("emails", "consumer-a", 1, 50, 100L)
  @test.assert_eq(leases.length(), 1)
  @test.assert_eq(backend.reap_expired_leases(151L), 1)

  let redelivered = backend.reserve_batch("emails", "consumer-b", 1, 50, 152L)
  @test.assert_eq(redelivered.length(), 1)
  @test.assert_eq(redelivered[0].job.job_id, "job-2")
}

///|
pub async fn[B : @backend.QueueBackend] assert_backend_contract_retry_visibility_window(
  backend : B,
) -> Unit {
  ignore(
    backend.enqueue(build_backend_contract_job("job-retry-1", "emails", 0L)),
  )

  let leases = backend.reserve_batch("emails", "consumer-a", 1, 100, 0L)
  @test.assert_eq(leases.length(), 1)
  backend.nack_retry(leases[0].lease_id, "retry later", 200L, 50L)

  let early = backend.reserve_batch("emails", "consumer-b", 1, 100, 199L)
  @test.assert_eq(early.length(), 0)

  let visible = backend.reserve_batch("emails", "consumer-b", 1, 100, 200L)
  @test.assert_eq(visible.length(), 1)
  @test.assert_eq(visible[0].job.job_id, "job-retry-1")
}

///|
pub async fn[B : @backend.QueueBackend] assert_backend_contract_retry_and_dedupe_lifecycle(
  backend : B,
) -> Unit {
  ignore(
    backend.enqueue(
      build_backend_contract_job("job-3", "emails", 0L, dedupe_key="same-key"),
    ),
  )
  let duplicate_result : Result[@core.JobId, Error] = try? backend.enqueue(
    build_backend_contract_job("job-4", "emails", 0L, dedupe_key="same-key"),
  )
  guard duplicate_result is Err(@core.QueueError::DuplicateJob("same-key")) else {
    fail("expected duplicate job to be rejected while active")
  }

  let leases = backend.reserve_batch("emails", "consumer-a", 1, 100, 0L)
  backend.nack_retry(leases[0].lease_id, "boom", 50L, 10L)

  let second_duplicate : Result[@core.JobId, Error] = try? backend.enqueue(
    build_backend_contract_job("job-5", "emails", 0L, dedupe_key="same-key"),
  )
  guard second_duplicate is Err(@core.QueueError::DuplicateJob("same-key")) else {
    fail("expected duplicate to stay blocked while retry waiting")
  }

  let retry_leases = backend.reserve_batch("emails", "consumer-a", 1, 100, 60L)
  backend.nack_dead(retry_leases[0].lease_id, "dead", 70L)
  let released_job_id = backend.enqueue(
    build_backend_contract_job("job-6", "emails", 0L, dedupe_key="same-key"),
  )
  @test.assert_eq(released_job_id, "job-6")
}

///|
pub async fn[B : @backend.QueueBackend] assert_backend_contract_extend_lease(
  backend : B,
) -> Unit {
  ignore(
    backend.enqueue(build_backend_contract_job("job-extend-1", "emails", 0L)),
  )

  let leases = backend.reserve_batch("emails", "consumer-a", 1, 100, 100L)
  @test.assert_eq(leases.length(), 1)
  backend.extend_lease(leases[0].lease_id, 300L)

  @test.assert_eq(backend.reap_expired_leases(200L), 0)
  let still_hidden = backend.reserve_batch("emails", "consumer-b", 1, 100, 200L)
  @test.assert_eq(still_hidden.length(), 0)

  @test.assert_eq(backend.reap_expired_leases(301L), 1)
  let redelivered = backend.reserve_batch("emails", "consumer-b", 1, 100, 302L)
  @test.assert_eq(redelivered.length(), 1)
  @test.assert_eq(redelivered[0].job.job_id, "job-extend-1")
}

///|
pub async fn[B : @backend.QueueBackend] assert_backend_contract_extend_missing_lease_is_noop(
  backend : B,
) -> Unit {
  ignore(
    backend.enqueue(
      build_backend_contract_job("job-extend-missing-1", "emails", 0L),
    ),
  )

  let leases = backend.reserve_batch("emails", "consumer-a", 1, 100, 100L)
  @test.assert_eq(leases.length(), 1)
  backend.ack(leases[0].lease_id, 120L)
  backend.extend_lease(leases[0].lease_id, 300L)

  let stats = backend.stats("emails")
  @test.assert_eq(stats.succeeded, 1)
  @test.assert_eq(stats.running, 0)
  @test.assert_eq(stats.reserved, 0)
}

///|
pub async fn[B : @backend.QueueBackend] assert_backend_contract_pause_and_resume(
  backend : B,
) -> Unit {
  ignore(backend.enqueue(build_backend_contract_job("job-7", "emails", 0L)))
  backend.pause("emails", "ops")
  @test.assert_eq(
    backend.reserve_batch("emails", "consumer-a", 1, 50, 0L).length(),
    0,
  )
  backend.resume_queue("emails")
  @test.assert_eq(
    backend.reserve_batch("emails", "consumer-a", 1, 50, 0L).length(),
    1,
  )
}
