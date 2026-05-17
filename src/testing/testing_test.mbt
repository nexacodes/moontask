///|
test "build_backend_contract_job marks future-visible jobs as scheduled" {
  let scheduled_job = build_backend_contract_job("job-scheduled", "emails", 50L)
  assert_true(scheduled_job.status is @core.Scheduled)

  let pending_job = build_backend_contract_job("job-pending", "emails", 0L)
  assert_true(pending_job.status is @core.Pending)
}

///|
async test "fake clock runtime tracks sleeps and manual advances" {
  let clock = new_fake_clock(10L)
  let runtime = clock.runtime_clock()

  @test.assert_eq((runtime.now_ms)(), 10L)
  (runtime.sleep_ms)(7)
  @async.pause()
  @test.assert_eq(clock.now_ms(), 17L)

  clock.advance(5)
  @test.assert_eq(clock.now_ms(), 22L)
  @test.assert_eq(clock.sleep_history(), [7])
  @test.assert_eq(clock.total_slept_ms(), 7)
}
