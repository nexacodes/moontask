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
