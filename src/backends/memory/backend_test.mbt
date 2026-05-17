///|
async test "mem backend reserves and acknowledges jobs" {
  try! @testing.assert_backend_contract_reserve_and_ack(new_mem_backend())
}

///|
async test "mem backend keeps a live lease exclusive to one consumer" {
  try! @testing.assert_backend_contract_lease_exclusivity(new_mem_backend())
}

///|
async test "mem backend redelivers expired leases" {
  try! @testing.assert_backend_contract_expired_lease_redelivery(
    new_mem_backend(),
  )
}

///|
async test "mem backend respects retry visibility windows" {
  try! @testing.assert_backend_contract_retry_visibility_window(
    new_mem_backend(),
  )
}

///|
async test "mem backend keeps retryable jobs active and releases dedupe on dead" {
  try! @testing.assert_backend_contract_retry_and_dedupe_lifecycle(
    new_mem_backend(),
  )
}

///|
async test "mem backend extends leases before redelivery" {
  try! @testing.assert_backend_contract_extend_lease(new_mem_backend())
}

///|
async test "mem backend ignores extend for completed leases" {
  try! @testing.assert_backend_contract_extend_missing_lease_is_noop(
    new_mem_backend(),
  )
}

///|
async test "mem backend pause blocks reservation until resumed" {
  try! @testing.assert_backend_contract_pause_and_resume(new_mem_backend())
}
