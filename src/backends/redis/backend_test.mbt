///|
async fn new_clean_backend(prefix : String) -> RedisBackend {
  let client = connect("127.0.0.1", 6379)
  defer client.close()
  let seq = match client.incr("moontask:test:redis:seq") {
    Ok(seq) => seq
    Err(err) =>
      raise @core.QueueError::BackendFailure(
        "INCR test prefix sequence: \{err}",
      )
  }
  new_redis_backend("127.0.0.1", 6379, prefix="\{prefix}:\{seq}")
}

///|
async fn new_clean_backend_with_pool(
  prefix : String,
  pool_size : Int,
) -> RedisBackend {
  let backend = try! new_clean_backend(prefix)
  new_redis_backend("127.0.0.1", 6379, prefix=backend.prefix, pool_size~)
}

///|
async test "redis backend reserves and acknowledges jobs" {
  try! @testing.assert_backend_contract_reserve_and_ack(
    try! new_clean_backend("moontask:test:redis:ack"),
  )
}

///|
async test "redis backend keeps a live lease exclusive to one consumer" {
  try! @testing.assert_backend_contract_lease_exclusivity(
    try! new_clean_backend("moontask:test:redis:lease"),
  )
}

///|
async test "redis backend redelivers expired leases" {
  try! @testing.assert_backend_contract_expired_lease_redelivery(
    try! new_clean_backend("moontask:test:redis:reap"),
  )
}

///|
async test "redis backend respects retry visibility windows" {
  try! @testing.assert_backend_contract_retry_visibility_window(
    try! new_clean_backend("moontask:test:redis:retry"),
  )
}

///|
async test "redis backend keeps retryable jobs active and releases dedupe on dead" {
  try! @testing.assert_backend_contract_retry_and_dedupe_lifecycle(
    try! new_clean_backend("moontask:test:redis:dedupe"),
  )
}

///|
async test "redis backend extends leases before redelivery" {
  try! @testing.assert_backend_contract_extend_lease(
    try! new_clean_backend("moontask:test:redis:extend"),
  )
}

///|
async test "redis backend ignores extend for completed leases" {
  try! @testing.assert_backend_contract_extend_missing_lease_is_noop(
    try! new_clean_backend("moontask:test:redis:extend-missing"),
  )
}

///|
async test "redis backend pause blocks reservation until resumed" {
  try! @testing.assert_backend_contract_pause_and_resume(
    try! new_clean_backend("moontask:test:redis:pause"),
  )
}

///|
async test "redis backend health reports redis connectivity" {
  let backend = try! new_clean_backend("moontask:test:redis:health")
  let health = try! backend.health()
  @test.assert_eq(health.ok, true)
  @test.assert_eq(health.backend_kind, "redis")
}

///|
async test "redis backend reuses pooled connections across sequential calls" {
  let backend = try! new_clean_backend_with_pool(
    "moontask:test:redis:pool-reuse", 1,
  )
  ignore(try! backend.health())
  @test.assert_eq(backend.pool.open_count, 1)
  ignore(try! backend.health())
  @test.assert_eq(backend.pool.open_count, 1)
}

///|
async test "redis backend pool blocks at configured maximum size" {
  let backend = try! new_clean_backend_with_pool(
    "moontask:test:redis:pool-bound", 2,
  )
  let client1 = backend.pool.acquire()
  let client2 = backend.pool.acquire()
  @test.assert_eq(backend.pool.open_count, 2)
  let events : Array[String] = []
  @async.with_task_group(async fn(group) {
    let waiting_client = group.spawn(
      async fn() {
        let client3 = backend.pool.acquire()
        defer client3.close()
        events.push("acquired")
        @test.assert_eq(client3.ping(), Ok("PONG"))
      },
      no_wait=true,
    )
    @async.sleep(20)
    @test.assert_eq(events.length(), 0)
    client1.close()
    @async.sleep(20)
    @test.assert_eq(events, ["acquired"])
    ignore(waiting_client.wait())
  })
  client2.close()
  @test.assert_eq(backend.pool.open_count, 2)
}
